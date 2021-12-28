from drama.manager import TaskManager
from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest
from drama.worker import execute


task_load = TaskRequest(
    name="LOADDATA",
    module="drama.core.catalog.load.ImportFile",
    params={"url": "http://0.0.0.0:9000/temperature/AEMETTemperaturaTest.xlsx"},
    inputs={},
)

task_matrix_transformation = TaskRequest(
    name="TEMPERATUREMATRIXTRANSFORMATION",
    module="drama_enbic2lab.catalog.water.TemperatureMatrixTransformation",
    params={},
    inputs={"TempFile": "LOADDATA.TempFile"},
)

task_statistical_max = TaskRequest(
    name="DATAEXTRACTIONMAX",
    module="drama_enbic2lab.catalog.water.DataExtraction",
    params={},
    inputs={"SimpleTabularDatasetMax": "TEMPERATUREMATRIXTRANSFORMATION.SimpleTabularDatasetMax"},
)
task_statistical_min = TaskRequest(
    name="DATAEXTRACTIONMIN",
    module="drama_enbic2lab.catalog.water.DataExtraction",
    params={},
    inputs={"SimpleTabularDatasetMin": "TEMPERATUREMATRIXTRANSFORMATION.SimpleTabularDatasetMin"},
)

task_completition = TaskRequest(
    name="TEMPERATURESERIESCOMPLETITION",
    module="drama_enbic2lab.catalog.water.TemperatureSeriesCompletition",
    params={
        "start_date": "1918-10-01",
        "end_date": "1921-09-30",
        "target_station": "QUESADA (FUENTE DEL PINO)",
        "analysis_stations": [
            "POZO ALCON (PRADOS DE CUENCA)",
            "POZO ALCON (EL HORNICO)",
        ],
        "priorize": "r2",
        "tests": ["pettit", "shnt", "buishand"],
    },
    inputs={
        "SimpleTabularDatasetMax": "TEMPERATUREMATRIXTRANSFORMATION.SimpleTabularDatasetMax",
        "SimpleTabularDatasetMin": "TEMPERATUREMATRIXTRANSFORMATION.SimpleTabularDatasetMin",
    },
)

workflow_request = WorkflowRequest(
    tasks=[
        task_load,
        task_matrix_transformation,
        task_statistical_max,
        task_statistical_min,
        task_completition,
    ]
)

workflow = execute(workflow_request)
print(workflow)

# gets results
print(TaskManager().find({"parent": workflow.id}))
