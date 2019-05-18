import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())


(p 
   | "Read the file" >> beam.io.ReadFromText('/mybeam/beam_programs/create_pipeline.py')
   | "Count length line" >> beam.FlatMap(lambda x: [len(x)])
   | "Write Output" >> beam.io.WriteToText('mybeam/pardo_short.txt')
)


p.run().wait_until_finish()


