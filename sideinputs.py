import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())

class ComputeWordLengthFn(beam.DoFn):
    def process(self,element):
        element = element.split(' ')
        element = [(len(i),i) for i in element if len(i)]
        return element

def find_rangeFn(element, lower, upper):
    if lower <= element[0] <= upper:
       return [element]

(p 
   | "Read the file" >> beam.io.ReadFromText('/mybeam/beam_programs/create_pipeline.py')
   | "lenght of line" >> beam.ParDo(ComputeWordLengthFn())
   | "find word between range" >> beam.FlatMap(find_rangeFn,10,100)
   | "Write Output" >> beam.io.WriteToText('mybeam/sideinputs.txt')
)


p.run().wait_until_finish()


