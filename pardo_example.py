import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())

class ComputeWordLengthFn(beam.DoFn):
    def process(self,element):
        return [len(element)]

(p 
   | "Read the file" >> beam.io.ReadFromText('/mybeam/beam_programs/create_pipeline.py')
   | "lenght of line" >> beam.ParDo(ComputeWordLengthFn())
   | "Write Output" >> beam.io.WriteToText('mybeam/pardo.txt')
)


p.run().wait_until_finish()


