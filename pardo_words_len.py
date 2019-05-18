import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())

class ComputeWordLengthFn(beam.DoFn):
    def process(self,element):
        element = element.split(' ')
        element = [(len(i),i) for i in element if len(i)]
        return element

(p 
   | "Read the file" >> beam.io.ReadFromText('/mybeam/beam_programs/create_pipeline.py')
   | "lenght of line" >> beam.ParDo(ComputeWordLengthFn())
   | "Write Output" >> beam.io.WriteToText('mybeam/pardo_words_len.txt')
)


p.run().wait_until_finish()


