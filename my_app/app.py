# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from io import BytesIO
import logging
import math
import random
import requests
import torch
from torchvision import models
from torchvision import transforms
import time
from typing import Iterable
from typing import Optional
from typing import Tuple
import apache_beam as beam
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.ml.inference import RunInference
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
# from apache_beam.transforms.periodicsequence import PeriodicImpulse
# from my_app.transformations.impulse import PeriodicImpulse
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterCount
from apache_beam.transforms.trigger import DefaultTrigger
from apache_beam.transforms.trigger import OrFinally
from apache_beam.transforms.trigger import Repeatedly
from apache_beam.utils import timestamp
from PIL import Image
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.runners import sdf_utils
from apache_beam.transforms import core
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp


# Everything surrounding impulsegen + periodic sequence/impulse can mostly be ignored.
# This is a local patch of a bug in Beam 2.42.0 which will be fixed shortly (likely in 2.44.0).
class ImpulseSeqGenRestrictionProvider(core.RestrictionProvider):
  def initial_restriction(self, element):
    start, end, interval = element
    if isinstance(start, Timestamp):
      start = start.micros / 1000000
    if isinstance(end, Timestamp):
      end = end.micros / 1000000

    assert start <= end
    assert interval > 0
    total_outputs = math.ceil((end - start) / interval)
    return OffsetRange(0, total_outputs)

  def create_tracker(self, restriction):
    return OffsetRestrictionTracker(restriction)

  def restriction_size(self, unused_element, restriction):
    return restriction.size()


class ImpulseSeqGenDoFn(beam.DoFn):
  '''
  ImpulseSeqGenDoFn fn receives tuple elements with three parts:

  * first_timestamp = first timestamp to output element for.
  * last_timestamp = last timestamp/time to output element for.
  * fire_interval = how often to fire an element.

  For each input element received, ImpulseSeqGenDoFn fn will start
  generating output elements in following pattern:

  * if element timestamp is less than current runtime then output element.
  * if element timestamp is greater than current runtime, wait until next
    element timestamp.

  ImpulseSeqGenDoFn can't guarantee that each element is output at exact time.
  ImpulseSeqGenDoFn guarantees that elements would not be output prior to
  given runtime timestamp.
  '''
  @beam.DoFn.unbounded_per_element()
  def process(
      self,
      element,
      restriction_tracker=beam.DoFn.RestrictionParam(
          ImpulseSeqGenRestrictionProvider()),
      watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
          ManualWatermarkEstimator.default_provider())):
    '''
    :param element: (start_timestamp, end_timestamp, interval)
    :param restriction_tracker:
    :return: yields elements at processing real-time intervals with value of
      target output timestamp for the element.
    '''
    start, _, interval = element

    if isinstance(start, Timestamp):
      start = start.micros / 1000000

    assert isinstance(restriction_tracker, sdf_utils.RestrictionTrackerView)

    current_output_index = restriction_tracker.current_restriction().start
    current_output_timestamp = start + interval * current_output_index
    current_time = time.time()
    watermark_estimator.set_watermark(
        timestamp.Timestamp(current_output_timestamp))

    while current_output_timestamp <= current_time:
      if restriction_tracker.try_claim(current_output_index):
        yield current_output_timestamp
        current_output_index += 1
        current_output_timestamp = start + interval * current_output_index
        current_time = time.time()
        watermark_estimator.set_watermark(
            timestamp.Timestamp(current_output_timestamp))
      else:
        return

    restriction_tracker.defer_remainder(
        timestamp.Timestamp(current_output_timestamp))


class PeriodicSequence(PTransform):
  '''
  PeriodicSequence transform receives tuple elements with three parts:

  * first_timestamp = first timestamp to output element for.
  * last_timestamp = last timestamp/time to output element for.
  * fire_interval = how often to fire an element.

  For each input element received, PeriodicSequence transform will start
  generating output elements in following pattern:

  * if element timestamp is less than current runtime then output element.
  * if element timestamp is greater than current runtime, wait until next
    element timestamp.

  PeriodicSequence can't guarantee that each element is output at exact time.
  PeriodicSequence guarantees that elements would not be output prior to given
  runtime timestamp.
  The PCollection generated by PeriodicSequence is unbounded.
  '''
  def __init__(self):
    pass

  def expand(self, pcoll):
    return (
        pcoll
        | 'GenSequence' >> beam.ParDo(ImpulseSeqGenDoFn())
        | 'MapToTimestamped' >> beam.Map(lambda tt: TimestampedValue(tt, tt)))


class PeriodicImpulse(PTransform):
  '''
  PeriodicImpulse transform generates an infinite sequence of elements with
  given runtime interval.

  PeriodicImpulse transform behaves same as {@link PeriodicSequence} transform,
  but can be used as first transform in pipeline.
  The PCollection generated by PeriodicImpulse is unbounded.
  '''
  def __init__(
      self,
      start_timestamp=Timestamp.now(),
      stop_timestamp=MAX_TIMESTAMP,
      fire_interval=360.0,
      apply_windowing=False):
    '''
    :param start_timestamp: Timestamp for first element.
    :param stop_timestamp: Timestamp after which no elements will be output.
    :param fire_interval: Interval at which to output elements.
    :param apply_windowing: Whether each element should be assigned to
      individual window. If false, all elements will reside in global window.
    '''
    self.start_ts = start_timestamp
    self.stop_ts = stop_timestamp
    self.interval = fire_interval
    self.apply_windowing = apply_windowing

  def expand(self, pbegin):
    result = (
        pbegin
        | 'ImpulseElement' >> beam.Create(
            [(self.start_ts, self.stop_ts, self.interval)])
        | 'GenSequence' >> beam.ParDo(ImpulseSeqGenDoFn())
        | 'MapToTimestamped' >> beam.Map(lambda tt: TimestampedValue(tt, tt)))
    if self.apply_windowing:
      result = result | 'ApplyWindowing' >> beam.WindowInto(
          window.FixedWindows(self.interval))
    return result


# Everything above can largely be ignored. This is a recreation of PeriodicImpulse because the built in version has a bug.

def read_image(image_url: str) -> Tuple[str, Image.Image]:
    response = requests.get(image_url)
    image = Image.open(BytesIO(response.content)).convert('RGB')
    return image_url, image

def preprocess_image(data: Image.Image) -> torch.Tensor:
  image_size = (224, 224)
  # Pre-trained PyTorch models expect input images normalized with the
  # below values (see: https://pytorch.org/vision/stable/models.html)
  normalize = transforms.Normalize(
      mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
  transform = transforms.Compose([
      transforms.Resize(image_size),
      transforms.ToTensor(),
      normalize,
  ])
  return transform(data)

class PostProcessor(beam.DoFn):
  def process(self, element: Tuple[str, PredictionResult]) -> Iterable[str]:
    image_url, prediction_result = element
    prediction = torch.argmax(prediction_result.inference, dim=0)
    # Map back to items based on https://deeplearning.cms.waikato.ac.nz/user-guide/class-maps/IMAGENET/
    if prediction.item() == 294:
        yield 'bear'
    elif prediction.item() == 309:
        yield 'bee'
    elif prediction.item() == 277:
        yield 'fox'
    elif prediction.item() == 235:
        yield 'dog'
    elif prediction.item() == 301:
        yield 'ladybug'
    elif prediction.item() == 327:
        yield 'starfish'
    elif prediction.item() == 292:
        yield 'tiger'
    elif prediction.item() == 340:
        yield 'zebra'
    else:
        yield 'no_matches'

class Logger(beam.DoFn):

  def __init__(self, name="main"):
    self._name = name

  def process(self, element, w=beam.DoFn.WindowParam,
              ts=beam.DoFn.TimestampParam):
    logging.warning('%s', element)
    yield element

@beam.DoFn.unbounded_per_element()
class AddWatermark(beam.DoFn):

    def process(self, element, ts=beam.DoFn.TimestampParam, restriction_tracker=beam.DoFn.RestrictionParam(
          ImpulseSeqGenRestrictionProvider()), watermark_estimator=beam.DoFn.WatermarkEstimatorParam(ManualWatermarkEstimator.default_provider())):
        logging.warning('Setting watermark: %s', ts)
        watermark_estimator.set_watermark(ts)
        yield element

class RandomlySampleImages(beam.DoFn):

    def __init__(self, example_images):
        self.example_images = example_images
    
    def process(self, unused_element):
        # Expecting 8,000 elements; this will return around 800 each time
        for tweet in self.example_images:
            num = random.random()
            # Sample 10% of the elements.
            if num > 0.9:
                yield tweet

def run(
    beam_options: Optional[PipelineOptions],
    example_images,
) -> None:
    model_class = models.mobilenet_v2
    model_params = {'num_classes': 1000}

    class PytorchModelHandlerTensorWithBatchSize(PytorchModelHandlerTensor):
        def batch_elements_kwargs(self):
            return {'min_batch_size': 10, 'max_batch_size': 100}

    # In this example we pass keyed inputs to RunInference transform.
    # Therefore, we use KeyedModelHandler wrapper over PytorchModelHandler.
    model_handler = KeyedModelHandler(
        PytorchModelHandlerTensorWithBatchSize(
            state_dict_path="gs://world-readable-mkcq69tkcu/dannymccormick/ato-model/mobilenet.pth",
            model_class=model_class,
            model_params=model_params))

    with beam.Pipeline(options=beam_options) as pipeline:
        it = time.time()
        duration = 300000
        et = it + duration
        interval = 1
        sample_image_urls = (
            pipeline
            | PeriodicImpulse(it, et, interval, False)
            | "Randomly sample images" >> beam.ParDo(RandomlySampleImages(example_images)))
        inferences = (
            sample_image_urls
            | beam.WindowInto(
                window.FixedWindows(1 * 60),
                # Fire trigger once we've accumulated at least 200 elements for a key
                trigger=OrFinally(Repeatedly(AfterCount(200)), DefaultTrigger()),
                accumulation_mode=AccumulationMode.ACCUMULATING)
            | 'ReadImageData' >> beam.Map(lambda image_url: read_image(image_url=image_url))
            | 'PreprocessImages' >> beam.MapTuple(lambda file_name, data: (file_name, preprocess_image(data)))
            | 'PyTorchRunInference' >> RunInference(model_handler)
            | 'ProcessOutput' >> beam.ParDo(PostProcessor()))
        (inferences
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'Count elements per key' >> beam.combiners.Count.PerKey()
        | 'LOG' >> beam.ParDo(Logger()))