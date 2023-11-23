class NotImplementedUseCaseError(Exception):
    pass


class NotImplementedIngestionMode(Exception):
    pass


class InvalidIngestionMode(Exception):
    pass


class PredictionAggregationError(Exception):
    pass


class MissingReferencePointId(Exception):
    pass


class PivotedPointIdColumnNameError(Exception):
    pass


class UnknownRandomLaw(Exception):
    pass
