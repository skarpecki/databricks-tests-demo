from pyspark.sql import DataFrame

class DataframesRegistry:
    """
    Registry of DataFrames. It associates an identifier (ADLS path, UC Table Name etc.) 
    with a mock DataFrame to be returned for this identifier.
    """
    def __init__(self):
        """
        Constructor
        """
        self.registry = {}

    def register_mock_dataframe(
            self,
            mock_df: DataFrame,
            identifier: str,
            overwrite: bool = False
        ) -> None:
        """
        Register a DataFrame for an identifier. If an identifier already registered
        it throws an error, unless you pass explicit overwrite flag.

        :param mock_df - DataFrame to be registered
        :param identifier - identifier under which mock_df is available
        :param ovewrite - default False - if method should overwrite a DataFrame if
            identifier already registered

        :raises ValueError - if identifier registered and method not told to overwrite
        """
        if identifier in self.registry and not overwrite:
            raise ValueError(f'DataFrame for {identifier} already exists. Please set overwrite param to True if intended.')
        self.registry[identifier] = mock_df

    def get_mock_dataframe(self, identifier: str) -> DataFrame:
        """
        Get Mock DataFrame for an identifier. If identifier not registered it raises 
        an error.

        :param identifier - Identifier for which DataFrame to return

        :raises ValueError - if identifier not registered
        """
        if identifier not in self.registry:
            raise ValueError(f"No mock DataFrame found for '{identifier}'.Please add one using 'register_mock_dataframe'.")
        return self.registry[identifier]