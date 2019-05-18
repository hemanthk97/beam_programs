import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())

def total_words(row):
    if "beam" in row:
        yield ('beam',row.count('beam'))

(p 
   | "Read the file" >> beam.io.ReadFromText('/mybeam/beam_programs/create_pipeline.py')
   | "Count length line" >> beam.Map(len)
   | "Write Output" >> beam.io.WriteToText('mybeam/pardo_short_map.txt')
)


p.run().wait_until_finish()


