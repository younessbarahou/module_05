from typing import Any, Protocol, Dict, Union
from abc import ABC, abstractmethod
import json


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        pass


class InputStage():
    def process(self, data: Any) -> Dict:
        try:
            if data["msg"]:
                print(f"Input: {data['msg']}")
                return (data)
        except KeyError:
            pass
        try:
            if data["csv"]:
                print("Input: ", end="")
                temp_buffer = data["csv"]
                print(f'"{temp_buffer}"')
                return (data)
        except KeyError:
            pass
        print(f"Input: {data}")
        return (data)


class TransformStage():
    def process(self, data: Any) -> Dict:
        try:
            if 'csv' in data:
                data = data.split(',')
                print("Transform: Parsed and structured data")
                return (data)
            elif 'msg' in data:
                print("Transform: Aggregated and filtered")
                return (data)
            else:
                if len(data) == 3 and 'sensor' in data and 'value' in data and 'unit' in data:
                    if data['value'] > 25:
                        data.update({'range': 'High range'})
                    if data['value'] == 25:
                        data.update({'range': 'Normal range'})
                    if data['value'] < 25:
                        data.update({'range': 'Low range'})
                    print("Transform: Enriched with metadata and validation")
                    return (data)
                else:
                    raise ValueError(
                        "Error detected in Stage 2: Invalid data format")
        except ValueError as e:
            print(e)


class OutputStage():
    def process(self, data: Any) -> str:
        if 'csv' in data:
            data = data.split(',')
            print("Transform: Parsed and structured data")
            return (data)
        elif 'msg' in data:
            print("Transform: Aggregated and filtered")
            return (data)
        else:
            if len(data) == 3 and 'sensor' in data and 'value' in data and 'unit' in data:
                if data['value'] > 25:
                    data.update({'range': 'High range'})
                if data['value'] == 25:
                    data.update({'range': 'Normal range'})
                if data['value'] < 25:
                    data.update({'range': 'Low range'})
                print("Transform: Enriched with metadata and validation")
                return (data)
            else:
                raise ValueError(
                    "Error detected in Stage 2: Invalid data format")


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
    def process(self, data: Any) -> Union[str, Any]:
        try:
            data = json.loads(data)
            if len(self.stages) == 0:
                raise ValueError("No Stages Added yet !")
            for stage in self.stages:
                data = stage.process(data)
        except (json.decoder.JSONDecodeError, TypeError):
            print("Error: Data is Invalid JSON !")
        except ValueError as e:
            print(e)


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        try:
            if type(data) is not str:
                raise ValueError()
            data.split(',')
            data = ({'csv': data})
        except (TypeError, ValueError):
            print("Error: Data in Invalid CSV !")
        if len(self.stages) == 0:
            raise ValueError("No Stages Added yet !")
        for stage in self.stages:
            data = stage.process(data)


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        try:
            if not isinstance(data, list):
                raise TypeError("Error: Data should be a list!")
            data = {
                "msg": "Real-time sensor stream",
                "length": len(data),
                "avg": sum(data) / len(data)
            }
        except TypeError as e:
            print(e)
        if len(self.stages) == 0:
            raise ValueError("No Stages Added yet !")
        for stage in self.stages:
            data = stage.process(data)


if __name__ == "__main__":
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    try:
        json_adapt = JSONAdapter("one")
        csv = CSVAdapter("two")
        sensor = StreamAdapter("three")
        stage1 = InputStage()
        stage2 = TransformStage()
        json_adapt.add_stage(stage1)
        json_adapt.add_stage(stage2)
        json_adapt.process('{"sensor": "two", "value": 23.5, "unit": "C"}')
        # csv.add_stage(stage1)
        # csv.process('one,two,three')
        # sensor.add_stage(stage1)
        # sensor.process("one")
    except Exception:
        print("awdi")
