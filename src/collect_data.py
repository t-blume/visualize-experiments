import json
from os import listdir
from os.path import isfile, join, isdir

import pandas as pd

FIND_AND_MERGE_STAGE_IDS = [6]


def collect_statistics(analyze_app, event_dir='/tmp/spark-events', memory_dir='/tmp/spark-memory'):
    captions = ["Iteration",
                "Incremental Start",
                "Incremental End",
                "Incremental Duration",
                "Incremental F&M Duration",
                "Batch Start",
                "Batch End",
                "Batch Duration",
                "Batch F&M Duration",
                'Memory Min',
                'Memory Max',
                'Memory Mean', 'direct-used']

    data_frame = pd.DataFrame(columns=captions)
    data_frame.set_index('Iteration')

    onlyfiles = [f for f in listdir(event_dir) if isfile(join(event_dir, f)) and 'inprogress' not in f]

    for file in onlyfiles:
        with open(join(event_dir, file)) as f:

            content = f.readlines()
            batch_computation = False
            iteration_number = -1
            for line in content:
                event = json.loads(line)

                if event['Event'] == 'SparkListenerEnvironmentUpdate':
                    app_name = event['Spark Properties']['spark.app.name']
                    if app_name.startswith(analyze_app):
                        version = app_name.replace(analyze_app, '')
                        if not version:
                            version = '0'
                        if '_batch_' in version:
                            batch_computation = True
                            iteration_number = int(version.replace('_batch_', ''))
                            if data_frame.loc[data_frame['Iteration'] == iteration_number].empty:
                                data_frame.loc[len(data_frame)] = iteration_number

                        else:
                            batch_computation = False
                            iteration_number = int(version)
                            if data_frame.loc[data_frame['Iteration'] == iteration_number].empty:
                                data_frame.loc[len(data_frame)] = iteration_number

                if event['Event'] == 'SparkListenerJobStart':
                    start = event['Submission Time']
                    caption = 'Incremental Start'
                    if batch_computation:
                        caption = 'Batch Start'

                    data_frame.loc[data_frame['Iteration'] == iteration_number, caption] = start

                if event['Event'] == 'SparkListenerJobEnd':
                    end = event['Completion Time']
                    caption = 'Incremental End'
                    if batch_computation:
                        caption = 'Batch End'
                    data_frame.loc[data_frame['Iteration'] == iteration_number, caption] = end
                if event['Event'] == 'SparkListenerStageCompleted':
                    if event['Stage Info']['Stage ID'] in FIND_AND_MERGE_STAGE_IDS:
                        caption = 'Incremental F&M Duration'
                        if batch_computation:
                            caption = 'Batch F&M Duration'

                        start = event['Stage Info']['Submission Time']
                        duration = event['Stage Info']['Completion Time'] - start
                        data_frame.loc[data_frame['Iteration'] == iteration_number, caption] = duration

            if isdir(memory_dir):
                """
                 '.driver.jvm.heap.committed.csv',
                 '.driver.jvm.heap.init.csv',
                 '.driver.jvm.heap.max.csv',
                 '.driver.jvm.heap.usage.csv',
                 '.driver.jvm.heap.used.csv',
                 .driver.BlockManager.memory.memUsed_MB.csv
                """
                # mem_df = pd.read_csv(join(memory_dir, file + '.driver.jvm.heap.used.csv'))
                # mem_df = pd.read_csv(join(memory_dir, file + '.driver.jvm.heap.usage.csv'))
                mem_df = pd.read_csv(join(memory_dir, file + '.driver.BlockManager.memory.memUsed_MB.csv'))
                data_frame.loc[data_frame['Iteration'] == iteration_number, 'Memory Min'] = mem_df['value'].min()
                data_frame.loc[data_frame['Iteration'] == iteration_number, 'Memory Max'] = mem_df['value'].max()
                data_frame.loc[data_frame['Iteration'] == iteration_number, 'Memory Mean'] = mem_df['value'].mean()
                mem_df2 = pd.read_csv(join(memory_dir, file + '.driver.jvm.direct.used.csv'))
                data_frame.loc[data_frame['Iteration'] == iteration_number, 'direct-used'] = mem_df2['value'].mean()

    data_frame['Incremental Duration'] = data_frame['Incremental End'] - data_frame['Incremental Start']
    data_frame['Batch Duration'] = data_frame['Batch End'] - data_frame['Batch Start']

    data_frame = data_frame.sort_values(['Iteration'], ascending=[True])
    data_frame = data_frame.set_index('Iteration')
    return data_frame
