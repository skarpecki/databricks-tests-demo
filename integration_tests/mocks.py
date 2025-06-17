from typing import Any, Optional, List, Optional, Self, Dict, Callable
from registry import DataframesRegistry
from dataclasses import dataclass, field
from pyspark.sql import DataFrame, SparkSession

# ---- Spark mocks helpers ---- #

class StreamingQueryMock:
    """
    Mock of streaming. Doesn't do anything. Just to have
    something in place
    """
    def __init__(self) -> Self:
        """
        Constructor
        """
        self.isActive = True
        self.exception = None
        self.lastProgress = None
        self.recentProgress = []

    def awaitTermination(self, *args, **kwrags) -> None:
        """
        Just to mimic original signature. Returns True always
        """
        return True

    def stop(self) -> None:
        """
        Just to mimic original signature. Just sets isActive to False
        """
        self.isActive = False

    def processAllAvailable(self) -> None:
        """
        Just to mimic original signature. Does nothing at all
        """
        pass
    

class ReaderWriterMockBase:
    """
    Base for Mocks containing common functionalites
    """
    def __init__(self):
        self._options = {}
        self._format = None

    def option(self, key: str, value: str) -> Self:
        """
        Option method to mimic original signature. Passed values
        are tracked in self._options
        """
        self._options[key] = value
        return self

    def options(self, **options) -> Self:
        """
        Options method to mimic original signature. Passed values
        are tracked in self._options
        """
        self._options.update(options)
        return self
    
    def format(self, format: str) -> Self:
        """
        Sets the format to mimic original signature. Format is tracked
        in self._format parameter.
        """
        self._format = format
        return self

# ---- Spark mocks ---- #

class SparkReaderMock(ReaderWriterMockBase):
    """
    Class to mock up behaviour of Spark DataFrameReader and DataStreamReader
    """
    def __init__(self, dataframe_registry: DataframesRegistry) -> Self:
        """
        Constructor

        :param dataframe_registry: Registry of DataFrames to be used by mock

        :return SparkReaderMock
        """
        super().__init__()

        self.dataframe_registry = dataframe_registry

        # For default methods (attributes) loading data use _fetch_mock method
        for name in ['load', 'parquet', 'json', 'csv', 'orc', 'text', 'avro', 'delta', 'table']:
            setattr(self, name, self._fetch_mock)

    def _fetch_mock(self, key, *args, **kwargs) -> DataFrame:
        """
        Internal method to fetch DataFrame from registry based on provided key.

        :param key: Key under which DataFrame was registered. Can be 
            either UC Name, Hive Name, Path or whatever else passed in registry.
            Every method loading data has either path or table name as first parameter
            thus key can be used here.

        :param *args: Just to fit the original methods' signtues
        :param **kwargs: Just to fit the original methods' signatuers
        
        :return DataFrame - STATIC DataFrame, no matter if Streaming or Static
            reader is being mocked.
        """
        return self.dataframe_registry.get_mock_dataframe(key)



class DataStreamWriterMock(ReaderWriterMockBase):
    """
    Mock up of DataStreamWriter. Note - this is not DataFrameWriter.
    This cannot be used to mock the static writer. If static mock
    is needed new class must be created.
    """
    def __init__(self, source_df: DataFrame) -> Self:
        """
        Constructor

        :param source_df - DataFrame on which mock is created
        """
        super().__init__()
        self._outputMode = None

        self.source_df = source_df
        self.trigger = DataStreamWriterTriggerMock(self)

    def outputMode(self, outputMode: str) -> Self:
        """
        Sets the output mode to mimic original signature. Mode is tracked
        in self._outputMode parameter.
        """
        self._outputMode = outputMode
        return self

    def foreachBatch(self, func: Callable) -> Self:
        """
        Mock up of foreachBatch method. It will apply provided function to 
        a DataFrame and return it once with a 1 as a batch_id. For further
        enhancements this could batch the DataFrame read to see if function
        correctly behaves in such an environments.

        :param func - Callable to apply to DataFrame
        """
        func(self.source_df, 1)
        return self

    def start(self, *args, **kwargs) -> StreamingQueryMock:
        """
        Mock up of streaming start.

        *args - to fit the original signature. Doesn't do anything with *args 
        """
        streaming_query = StreamingQueryMock()
        return streaming_query
    
class DataStreamWriterTriggerMock:
    """
    Mock up of DataStreamWritter trigger. Created just for chaining
    purpose and to override the original method 
    """
    def __init__(self, writer: DataStreamWriterMock) -> Self:
        """
        Constructor
        """
        self.processingTime = None
        self.once = None
        self.continuous = None
        self.availableNow = None
        self._writer = writer

    def __call__(self, *args, **kwargs) -> None:
        """
        To allow calling objet like a function
        """
        return self._writer
    

# ---- End of Spark mocks ---- #

# ---- DBUtils mocks ---- #
class DBUtilsMock:
    """
    Base DBUtils mock up. Can be extended to support more features.
    Now it supports just the Widgets module
    """
    def __init__(self) -> Self:
        """
        Constructor
        """
        self.widgets = Widgets()

    def update_widgets_parameters(self, parameters: dict) -> Self:
        """
        Set the widgets parameters to values provided in parameters

        :param parameters - Dict of widget_name: widget_value. 
            Whenever code asks for dbutils.widgets.get(widget_name)
            it returns associated value.

        :returns self - for chaining if needed.
        """
        self.widgets._update_parameters(parameters)
        return self

class Widgets:
    """
    Widgets mock up. Can be extended to allow more types of controls
    and to have more widgets methods 
    """
    def __init__(self) -> Self:
        """
        Constructor
        """
        self.parameters = {}

    def _update_parameters(self, parameters: dict) -> None:
        """
        Internal method to set the widgets parameters to values
        provided in parameters

        :param parameters - Dict of widget_name: widget_value. 
            Whenever code asks for dbutils.widgets.get(widget_name)
            it returns associated value.

        :returns self - for chaining if needed.
        """
        self.parameters.update(parameters)

    def _add_param(self, name: str, value: str) -> None:
        """
        Internal method to add a single parameter. It follows the original
        chain of DBUtils widgets, so adds it only if such parameter not yet
        exists. Used to add widgets default values.

        :param name - name of the widget
        :param value - value to be returned.
        """
        val = self.parameters.get(name)
        if val is None:
            self.parameters[name] = value

    def get(self, name: str) -> str:
        """
        To get widget value.

        :param name - name of the widget
        """
        return self.parameters[name]

    def text(self, name: str, defaultValue: str, label: Optional[str]=None) -> None:
        """
        Text method to follow original signature. Adds widget name with a default value
        if not already exists.

        :param name - name of the widget
        :param default_value - default value returned by the widget
        :param label - ignored
        """
        self._add_param(name, defaultValue)

    def dropdown(
            self,
            name: str,
            defaultValue: str,
            choices: List[str],
            label: Optional[str]=None
    ) -> None:
        """
        Dropdown method to follow original signature. Adds widget name with a default value
        if not already exists.

        :param name - name of the widget
        :param default_value - default value returned by the widget
        :param choices - ignored
        :param label - ignored
        """
        self._add_param(name, defaultValue)
        
    def removeAll(*args, **kwargs):
        pass


# ---- End of DBUtils mocks ---- #

# ---- DeltaTable mocks ---- #
@dataclass
class MergeStep:
    """
    Class defining a single merge step such as 'whenMatchedUpdate', 
    'whenMatchedUpdateAll' or 'whenMatchedInsert' etc.

    :var kind - Type of step, e.g. 'whenMatchedInsert'
    :var params - Parameters provided to a single step.
        E.g. set of columns to update 
    """
    kind: str 
    params: Dict[str, Any] = field(default_factory=dict)

class DeltaMergeMock:
    """ 
    Class to nock up a Merge call on a DeltaTable. 
    """
    def __init__(
            self,
            source: DataFrame,
            condition: str
        ) -> Self:
        """
        Constructor

        :param source - DataFrame which will be merged
        :param condition - Condition of a merge
        """
        self.source = source
        self.condition = condition
        self.steps = []
        self.executed = False

    def whenMatchedUpdate(
            self,
            condition: Optional[str]=None,
            set: Optional[Dict[str, Any]]=None
        ) -> Self:
        """
        Method to match original whenMatchedUpdate. Step will be added to self.steps 
            list and parameters will be tracked in merge step 
        """
        self.steps.append(
            MergeStep(
                kind='whenMatchedUpdate',
                params={'condition': condition, 'set': set}
            ))
        
        return self

    def whenMatchedUpdateAll(
            self,
            condition: Optional[str]=None
        ) -> Self:
        """
        Method to match original whenMatchedUpdateAll. Step will be added to self.steps 
            list and parameters will be tracked in merge step 
        """
        self.steps.append(
            MergeStep(
                kind='whenMatchedUpdateAll',
                params={'condition': condition}
            ))
        
        return self

    def whenNotMatchedInsertAll(self) -> Self:
        """
        Method to match original whenNotMatchedInsertAll. Step will be added to self.steps 
            list and parameters will be tracked in merge step 
        """
        self.steps.append(MergeStep(kind='whenNotMatchedInsertAll'))

        return self

    def whenNotMatchedInsert(self, values: Dict[str, Any]) -> Self:
        """
        Method to match original whenNotMatchedInsert. Step will be added to self.steps 
            list and parameters will be tracked in merge step 
        """
        self.steps.append(
            MergeStep(
                kind='whenNotMatchedInsert',
                params={'values': values}
            ))
        
        return self

    def whenMatchedDelete(self, condition: Optional[str]=None):
        """
        Method to match original whenMatchedDelete. Step will be added to self.steps 
            list and parameters will be tracked in merge step 
        """
        self.steps.append(
            MergeStep(
                kind='whenMatchedDelete',
                params={'condition': condition}
            ))
        
        return self

    def execute(self) -> None:
        """
        Execute the merge. Nothins happens in mock but flag self.executed is 
        set to True. No changes will be made to any Delta Table.
        """
        self.executed = True



class DeltaTableMock:
    """
    Mock of a single DeltaTable instance. 
    """
    def __init__(
            self,
            identifier: str,
            df_registry: DataframesRegistry
        ) -> None:
        """
        Constructor

        :param identifier - name of the Delta Table - catalog.schema.table
        :param df_registry -  registry of DataFrames to be used by mock.
        """
        self.identifier = identifier
        self.df_registry = df_registry
        self._alias = None
        self.merges = []

    def alias(self, name: str) -> Any:
        """
        Sets an alias as in original Delta Table. It tracks alias in self._alias
        """
        self._alias = name
        return self

    def merge(self, source: DataFrame, condition: str) -> DeltaMergeMock:
        """
        Initialize Merge action. Internally this creates DeltaMergeMock which can
        later be used to perfrom merge actions, e.g. 'whenMatchedUpdate' and which 
        tracks the merges.

        :param source - Source DataFrame to be used in merge
        :param condition - condition for a merge. This is simplest version accepting
            a str. However it can be extended to accept e.g. pyspark Col
        """
        merge = DeltaMergeMock(source, condition)
        self.merges.append(merge)

        return merge

    def toDF(self) -> DataFrame:
        """
        Returns a DataFrame registered for self.identifier.
        """
        return self.df_registry.get_mock_dataframe(self.identifier)


class DeltaTableMockGenerator:
    """
    Generator of DeltaTables. Needed to simplify logic of creating
    a DeltaTable mock via forName and forPath
    """
    def __init__(self, df_registry: DataframesRegistry) -> Self:
        """
        Constructor

        :param df_registry - DataFrameRegistry which will be later passed to each
            DeltaTableMock instance
        """
        self.df_registry = df_registry
        self.instances = []

    def forName(self, spark: SparkSession, table_name: str) -> DeltaTableMock:
        """
        Method that creates DeltaTableMock for a table_name.

        :param spark - Ignored. To match original signature
        :param table_name - Name of the table for which DeltaTable is created
        """
        mock_obj = DeltaTableMock(identifier=table_name, df_registry=self.df_registry)
        self.instances.append(mock_obj)
        return mock_obj

    def forPath(self, spark: Any, path: str) -> DeltaTableMock:
        """
        Method that creates DeltaTableMock for a path.

        :param spark - Ignored. To match original signature
        :param path - Path to the table files for which DeltaTable is created
        """
        mock_obj = DeltaTableMock(identifier=str(path), df_registry=self.df_registry)
        self.instances.append(mock_obj)
        return mock_obj
    
# ---- End of DeltaTable mocks ---- #