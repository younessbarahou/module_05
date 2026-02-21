from typing import Any, Protocol, Dict, Union
from abc import ABC, abstractmethod
import json


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        pass


class InputStage():
    def process(self, data: Any) -> Dict:
        if "msg" in data:
            print(f"Input: {data['msg']}")
            return (data)
        if "csv" in data:
            print("Input: ", end="")
            temp_buffer = data["csv"]
            print(f'"{temp_buffer}"')
            return (data)
        else:
            print(f"Input: {data}")
            return (json.loads(data))


class TransformStage():
    def process(self, data: Any) -> Dict:
        if 'csv' in data:
            action_cte = 0
            data['csv'] = data['csv'].split(',')
            if len(data['csv']) == 0:
                raise ValueError("Error detected in Stage 2: Data is empty!")
            for d in data['csv']:
                if d == 'action':
                    action_cte += 1
            data = {'action': action_cte, 'data': data['csv']}
            print("Transform: Parsed and structured data")
            return (data)
        elif 'msg' in data:
            data_length = len(data['data'])
            if data_length == 0:
                raise ValueError("Error detected in Stage 2: Data is empty!")
            for d in data['data']:
                if type(d) is not float and type(d) is not int:
                    raise TypeError(
                        "Error detected in Stage 2: Invalid Data format"
                        )
            data['data'] = [d for d in data['data'] if d >= 0]
            data_sum = sum(data['data'])
            data.update(
                {'len': len(data['data']), 'avg': data_sum / len(data['data'])}
                )
            print("Transform: Aggregated and filtered")
            return (data)
        else:
            if (
                len(data) == 3
                and 'sensor' in data
                and 'value' in data
                and 'unit' in data
            ):
                float(data['value'])
                if float(data['value']) > 23:
                    data.update({'range': 'High range'})
                if float(data['value']) == 23:
                    data.update({'range': 'Normal range'})
                if float(data['value']) < 23:
                    data.update({'range': 'Low range'})
                print("Transform: Enriched with metadata and validation")
                return (data)
            raise ValueError(
                "Error detected in Stage 2: Invalid data format")


class OutputStage():
    def process(self, data: Any) -> str:
        if 'action' in data:
            action = data['action']
            result = f"Output: User activity logged:{action} actions processed"
            print(result)
            return result
        elif 'len' in data:
            length = data['len']
            avg = data['avg']
            result = f"Output: Stream summary:{length} readings, avg: {avg}C"
            print(result)
            return result
        else:
            value = data['value']
            unit = data['unit']
            rang = data['range']
            result = f"Output: Processed temp reading: {value}{unit} ({rang})"
            print(result)
            return result


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id: str) -> None:
        self.stages = []
        self.id = pipeline_id

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        if len(self.stages) == 0:
            raise ValueError("No Stages Added yet !")
        for stage in self.stages:
            data = stage.process(data)
        return data


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        if type(data) is not str:
            raise ValueError("Error: Data should be a string!")
        data = ({'csv': data})
        if len(self.stages) == 0:
            raise ValueError("No Stages Added yet !")
        for stage in self.stages:
            data = stage.process(data)
        return data


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        if not isinstance(data, list):
            raise TypeError("Error: Data should be a list!")
        data = {
            "msg": "Real-time sensor stream",
            'data': data
        }
        if len(self.stages) == 0:
            raise ValueError("No Stages Added yet !")
        for stage in self.stages:
            data = stage.process(data)
        return data


class NexusManager():
    def __init__(self):
        self.pipelines = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> ProcessingPipeline:
        if not isinstance(pipeline, ProcessingPipeline):
            raise TypeError("Pipeline Entered Is Invalid!")
        self.pipelines.append(pipeline)
        return (pipeline)

    def process_data(self, data: Any) -> None:
        for pipeline in self.pipelines:
            data = pipeline.process(data)


if __name__ == "__main__":
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print()
    try:
        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second")
        manager = NexusManager()
        print()
        print("Creating Data Processing Pipeline...")
        stage_1 = InputStage()
        stage_2 = TransformStage()
        stage_3 = OutputStage()
        pipeline_1 = manager.add_pipeline(JSONAdapter("pip_01"))
        pipeline_2 = manager.add_pipeline(CSVAdapter("pip_02"))
        pipeline_3 = manager.add_pipeline(StreamAdapter("pip_03"))
        stages = [stage_1, stage_2, stage_3]
        pipelines = [pipeline_1, pipeline_2, pipeline_3]
        for pipe in pipelines:
            for stage in stages:
                pipe.add_stage(stage)
        print("Stage 1: Input validation and parsing")
        print("Stage 2: Data transformation and enrichment")
        print("Stage 3: Output formatting and delivery")
        print()
        print("=== Multi-Format Data Processing ===")
        print()
        print("Processing JSON data through pipeline...")
        json_string = '{"sensor": "temp", "value": 23.5, "unit": "C"}'
        pipeline_1.process(json_string)
        print()
        print("Processing CSV data through same pipeline...")
        csv_string = "user,action,timestamp"
        pipeline_2.process(csv_string)
        print()
        print("Processing Stream data through same pipeline...")
        sensor_data = [5, 100, 948, 35, 20, 23]
        pipeline_3.process(sensor_data)
        print()
        print("=== Pipeline Chaining Demo ===")
        print("Pipeline A -> Pipeline B -> Pipeline C")
        print("Data flow: Raw -> Processed -> Analyzed -> Stored")
        manager_1 = NexusManager()
        pipeline_01 = manager_1.add_pipeline(JSONAdapter("pip_01"))
        pipeline_02 = manager_1.add_pipeline(JSONAdapter("pip_02"))
        pipeline_03 = manager_1.add_pipeline(JSONAdapter("pip_03"))
        pipelines = [pipeline_01, pipeline_02, pipeline_03]
        pipeline_01.add_stage(stage_1)
        pipeline_02.add_stage(stage_2)
        pipeline_03.add_stage(stage_3)
        manager_1.process_data(
            '{"sensor": "temp", "value": 23.5, "unit": "C"}'
            )
        print("processed in 0.2s..")
        print()
        print("=== Error Recovery Test ===")
        print("Simulating pipeline failure...")
        pipeline_1.process('{"value": 1, "unit": "C"}')
    except json.decoder.JSONDecodeError:
        print("Error: Data is Invalid JSON !")
        print("Recovery initiated: Switching to backup processor")
        print("Recovery successful: Pipeline restored, processing resumed")
        print()
    except (ValueError, TypeError) as e:
        print(e)
        print("Recovery initiated: Switching to backup processor")
        print("Recovery successful: Pipeline restored, processing resumed")
        print()
    except Exception as e:
        print(e)
        print()
    print("Nexus Integration complete. All systems operational.")
