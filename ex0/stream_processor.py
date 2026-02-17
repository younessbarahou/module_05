from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return (f"Output: {result}")


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        self.length = 0
        self.sum = 0
        self.avg = 0

    def validate(self, data: Any) -> bool:
        if type(data) is not list or len(data) == 0:
            return (False)
        for num in data:
            if type(num) is not int:
                return (False)
        return (True)

    def process(self, data: Any) -> str:
        try:
            if self.validate(data) is False:
                raise TypeError(
                    "Processing Failed, list of numeric values is required!"
                    )
            self.length = len(data)
            self.sum = sum(data)
            self.avg = self.sum / self.length
            return (
                f"Processed {self.length} numeric values, sum = {self.sum}, "
                f"avg ={self.avg}"
                )
        except TypeError as e:
            return (e)

    def format_output(self, result: str) -> str:
        return (super().format_output(result))


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        self.length = 0
        self.words = 0

    def validate(self, data: Any) -> bool:
        if (type(data) is not str):
            return (False)
        return (True)

    def process(self, data: Any) -> str:
        try:
            if (self.validate(data) is False):
                raise TypeError("Processing Failed, string is required!")
            self.length = len(data)
            self.words = len(data.split(' '))
            return (
                f"Processed text: {self.length} characters, {self.words} words"
                )
        except TypeError as e:
            return (e)

    def format_output(self, result: str) -> str:
        return (super().format_output(result))


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        self.log = []

    def validate(self, data: Any) -> bool:
        if (type(data) is not str):
            return (False)
        self.log = data.split(": ")
        if (len(self.log) != 2):
            return (False)
        return (True)

    def process(self, data: Any) -> str:
        if self.validate(data) is False:
            return ("Processing Failed")
        else:
            if self.log[0] == "ERROR":
                return (f"[ALERT] {self.log[0]} level detected: {self.log[1]}")
            elif self.log[0] == "INFO":
                return (f"[INFO] {self.log[0]} level detected: {self.log[1]}")

    def format_output(self, result: str) -> str:
        return (super().format_output(result))


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()
    print("Initializing Numeric Processor...")
    value = [1, 2, 3, 1]
    num_processor = NumericProcessor()
    print(f"Processing data: {value}")
    processed = num_processor.process(value)
    if num_processor.validate(value) is True:
        print("Validation: Numeric Data Verified")
    else:
        print("Validation: Numeric data Unverified")
    print(num_processor.format_output(processed))
    print()
    print("Initializing Text Processor...")
    text = "hello world"
    text_processor = TextProcessor()
    print(f"Processing data: {text}")
    processed_txt = text_processor.process(text)
    if text_processor.validate(text) is True:
        print("Validation: Text Data verified")
    else:
        print("Validation: Text Data Unverified")
    print(text_processor.format_output(processed_txt))
    print()
    print("Initializing Log Processor...")
    log = "ERROR: Connection timeout"
    log_processor = LogProcessor()
    print(f"Processing data: {log}")
    processed_log = log_processor.process(log)
    if log_processor.validate(log) is True:
        print("Validation: Log Data verified")
    else:
        print("Validation: Log Data Unverified")
    print(text_processor.format_output(processed_log))
    print()
    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    multiple_instances = [NumericProcessor(), TextProcessor(), LogProcessor()]
    multiple_data = [[100, 200, 300], "Code Nexus", "INFO: System Ready"]
    index = 1
    for instance, data in zip(multiple_instances, multiple_data):
        print(f"Result {index}: ", end="")
        print(instance.process(data))
        index += 1
    print()
    print("Foundation systems online. Nexus ready for advanced streams.")
