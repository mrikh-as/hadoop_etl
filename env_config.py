from dataclasses import dataclass
import os
import sys


@dataclass
class EnvConfig:
    spark_major_version: str
    spark_home: str
    spark_python_path: str
    py4j_path: str

    @classmethod
    def from_env(cls):
        return cls(
            spark_major_version=os.getenv("SPARK_MAJOR_VERSION", "3.5.1"),
            spark_home=os.getenv("SPARK_HOME", "/usr/sdp/current/spark3.5.1-client"),
            spark_python_path=os.getenv(
                "SPARK_PYTHON_PATH", "/usr/sdp/current/spark3.5.1-client/python/"
            ),
            py4j_path=os.getenv(
                "PY4J_PATH",
                "/usr/sdp/current/spark3.5.1-client/python/lib/py4j-0.10.9.7-src.zip",
            ),
        )

    def setup(self):
        os.environ["SPARK_MAJOR_VERSION"] = self.spark_major_version
        os.environ["SPARK_HOME"] = self.spark_home

        python_path = sys.executable
        os.environ["PYSPARK_PYTHON"] = python_path
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

        sys.path.insert(0, self.spark_python_path)
        sys.path.insert(0, self.py4j_path)
