
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())

# example of composite transforms
class Countwords(beam.PTransform):
    def expand(self,pcoll):
        return (pcoll
                | "ExtractWords" >> beam.FlatMap(lambda x: x.split(' '))
                | beam.combiners.Count.PerElement())

lines = p | beam.io.ReadFromText('/mybeam/beam_programs/create_pipeline.py')
counts = lines | Countwords()
counts | beam.io.WriteToText('mybeam/composite.txt') 


p.run().wait_until_finish()
