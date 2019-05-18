import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())

class ComputeWordLengthFn(beam.DoFn):
    def process(self,element):
        element = element.split(' ')
        element = [(i,len(i)) for i in element if len(i)]
        return element

(p 
   | "Read the file" >> beam.io.ReadFromText('/mybeam/beam_programs/create_pipeline.py')
   | "lenght of line" >> beam.ParDo(ComputeWordLengthFn())
   | "Grouping by words" >> beam.GroupByKey()
   | "Write Output" >> beam.io.WriteToText('mybeam/pardo_words_len_groupbykey.txt')
)


p.run().wait_until_finish()


