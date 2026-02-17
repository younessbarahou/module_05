from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union, Dict


class DataStream(ABC):
    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        if (not isinstance(data_batch, list) or len(data_batch) == 0):
            raise TypeError()
        self.data_length = len(data_batch)

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] =
                    None) -> List[Any]:
        try:
            if not isinstance(criteria, str) and criteria is not None:
                raise TypeError()
            if criteria is None:
                return data_batch
            elif criteria == "High":
                return [
                    data
                    for data in data_batch
                    if float(data.split(":")[1]) >= 50
                    ]
            elif criteria == "Medium":
                return [
                    data
                    for data in data_batch
                    if float(data.split(":")[1]) == 25
                    ]
            elif criteria == "Low":
                return [
                    data
                    for data in data_batch
                    if float(data.split(":")[1]) < 25
                    ]
        except TypeError:
            return (["Criteria should be string / None"])
        except ValueError:
            return (["Data Is Invalid ! Hint=>['str:number']..."])

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.data_type = "Enviromental Data"
        self.data_length = 0
        self.data_splitted = []

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            super().process_batch(data_batch)
            for data in data_batch:
                temp_buffer = data.split(':')
                if len(temp_buffer) < 2:
                    raise ValueError()
            self.data_splitted = [element.split(':') for element in data_batch]
            for splitted in self.data_splitted:
                if not isinstance(splitted[0], str):
                    raise ValueError()
                float(splitted[1])
            return (
                f"{self.data_length} readings processed"
                )
        except (TypeError, ValueError):
            return ("Data Entered Invalid !\nHint=> ['string1:number1'...]")

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] =
                    None) -> List[Any]:
        return (super().filter_data(data_batch, criteria))

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return ({'key': 'avg',
                 self.data_splitted[0][0]: self.data_splitted[0][1]})


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.data_type = "Financial Data"
        self.data_splitted = {}
        self.data_net = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            super().process_batch(data_batch)
            self.data_splitted = [element.split(":") for element in data_batch]
            for splitted in self.data_splitted:
                if isinstance(splitted[0], str) is False:
                    raise ValueError()
                temp_buffer = int(splitted[1])
                if temp_buffer < 0:
                    raise ValueError()
            for element in self.data_splitted:
                if element[0] == "buy":
                    self.data_net += int(element[1])
                elif element[0] == "sell":
                    self.data_net -= int(element[1])
            return (f"{self.data_length} operations processed")
        except (TypeError, ValueError):
            return ("Data Invalid\nHint=>['string1:positive number1'...]")

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] =
                    None) -> List[Any]:
        return (super().filter_data(data_batch, criteria))

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        if self.data_net >= 0:
            return ({"net": f"+{self.data_net}"})
        else:
            return ({"net": self.data_net})


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.data_length = 0
        self.data_error = 0
        self.data_type = "System Events"

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            super().process_batch(data_batch)
            for element in data_batch:
                if not isinstance(element, str):
                    raise ValueError()
            for element in data_batch:
                if element == "error":
                    self.data_error += 1
            return (f"{self.data_length} events processed")
        except (TypeError, ValueError):
            return ("Data Entered Invalid!\nHint=> ['str1', 'str2'...]")

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] =
                    None) -> List[Any]:
        try:
            if not isinstance(criteria, str) and criteria is not None:
                raise TypeError()
            if criteria is None:
                return data_batch
            elif criteria == "High":
                return [data for data in data_batch if data == "error"]
            elif criteria == "Medium":
                return [data for data in data_batch if data == "logout"]
            elif criteria == "Low":
                return [data for data in data_batch if data == "login"]
        except TypeError:
            return (["Criteria should be string / None"])

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return ({'error': self.data_error})


class StreamProcessor():
    def __init__(self, buff_ids: list[str]) -> None:
        self.s_types = {
            'sensor': SensorStream,
            'transaction': TransactionStream,
            'event': EventStream
            }
        self.ids = buff_ids

    def process_all(self, stream_data: List[List[Any]]) -> None:
        try:
            if not isinstance(stream_data, list) or len(stream_data) == 0:
                raise TypeError()
            for (stream_type, id, data) in zip(
                self.s_types, self.ids, stream_data
            ):
                temp_buff = self.s_types[stream_type](id)
                result = temp_buff.process_batch(data)
                print(f"{stream_type} data: {result}")
        except TypeError:
            print("Data Invalide\nHint=>[[data],...]")

    def filter_all(
            self, stream_data: List[List[Any]], criteria: list[str]
            ) -> None:
        try:
            result = {}
            if not isinstance(stream_data, list) or len(stream_data) == 0:
                raise TypeError()
            for (stream_type, id, data, crit) in zip(self.s_types,
                                                     self.ids,
                                                     stream_data,
                                                     criteria):
                temp_buff = self.s_types[stream_type](id)
                result_buff = temp_buff.filter_data(data, crit)
                result.update({stream_type: len(result_buff)})
            result = {res: result[res] for res in result if result[res] > 0}
            for element in result:
                if element == 'sensor':
                    print(f"{result[element]} critical sensor alerts,", end="")
                elif element == 'transaction':
                    print(f"{result[element]} large transaction,", end="")
                elif element == 'event':
                    print(f"{result[element]} critical event alerts", end="")
        except TypeError:
            print("Data Invalide\nHint=>[[data],...]")
        except Exception as e:
            print(e)


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()
    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    sensor_data = ["temp:22.5", "humidity:65", "pressure:123"]
    print(f"Stream ID: {sensor.stream_id}", f"Type: {sensor.data_type}")
    print(f"Processing sensor batch: {sensor_data}")
    process = sensor.process_batch(sensor_data)
    sensor_stats = sensor.get_stats()
    print(
        f"Sensor Analysis: {process},",
        f"{sensor_stats['key']}: {sensor_stats['temp']}"
        )
    print()
    print("Initializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    transaction_data = ["buy:100", "sell:150", "buy:75"]
    print(f"Stream ID: {transaction.stream_id}",
          f"Type: {transaction.data_type}"
          )
    print(f"Processing Transaction batch: {transaction_data}")
    process = transaction.process_batch(transaction_data)
    transaction_stats = transaction.get_stats()
    print(f"Transaction analysis: {process}, {transaction_stats['net']} units")
    print()
    print("Initializing Event Stream...")
    event = EventStream("EVENT_001")
    event_data = ['login', 'error', 'logout']
    print(f"Stream ID: {event.stream_id}", f"Type: {event.data_type}")
    print(f"Processing sensor batch: {event_data}")
    process = event.process_batch(event_data)
    event_stats = event.get_stats()
    print(f"Event analysis: {process}, {event_stats['error']} error detected")
    print()
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    buff_ids = ["001", "002", "003"]
    orchestrator = StreamProcessor(buff_ids)
    print()
    print("Batch 1 Results:")
    orchestrator.process_all([sensor_data, transaction_data, event_data])
    print()
    criteria = ["High", "Low", "High"]
    print(f"Stream filtering active: {criteria[0]}-priority data only")
    print("Filtered results: ", end="")
    orchestrator.filter_all(
        [sensor_data, transaction_data, event_data], criteria
    )
    print()
    print()
    print("All streams processed successfully. Nexus throughput optimal.")
