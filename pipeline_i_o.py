

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())

# example of composite transforms
class Countwords(beam.PTransform):
    def expand(self,pcoll):
        return (pcoll
                | "ExtractWords" >> beam.FlatMap(lambda x: x.split(' '))
                | beam.combiners.Count.PerElement()
                | "Format data to CSV" >> beam.FlatMap(lambda x:[""+x[0]+";"+str(x[1])+""]))
      
lines = p | beam.io.ReadFromText('/mybeam/beam_programs/*.py')
counts = lines | Countwords()
counts | beam.io.WriteToText('mybeam/composite', file_name_suffix=".csv") 



p.run().wait_until_finish()
