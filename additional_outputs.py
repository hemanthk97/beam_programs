

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())

class ComputeWordLengthFn(beam.DoFn):
    def process(self,element):
        element = element.split(' ')
        element = [(len(i),i) for i in element if len(i)]
        return element

class MultipleOP(beam.DoFn):
    def process(self,element,num):
        if element[0] < num:
           yield [element]
        else:
           yield pvalue.TaggedOutput('Less_10',[element])


ten, gten = (p 
   | "Read the file" >> beam.io.ReadFromText('/mybeam/beam_programs/create_pipeline.py')
   | "lenght of line" >> beam.ParDo(ComputeWordLengthFn())
   | "Multiple outputs" >> beam.ParDo(MultipleOP(),10).with_outputs('Less_10',main="great_10"))


ten | "less ten output" >> beam.io.WriteToText('mybeam/less_ten.txt')
gten | "Great ten" >>  beam.io.WriteToText('mybeam/great_ten.txt')



p.run().wait_until_finish()


