class Functions:
    template_sql = None
    allow_aggregration = False

    def __init__(self, field_name):
        self.field_name = field_name

    def __repr__(self):
        return f'{self.__class__.__name__}({self.field_name})'

    @property
    def alias_field_name(self):
        """Potential alias name that can be used
        if this function is not used via an 
        explicit alias"""
        return f'{self.__class__.__name__.lower()}_{self.field_name}'

    @staticmethod
    def create_function(connection):
        """Use this function to register a local
        none existing function in the database
        function space in other to use none
        conventional functions"""
        return NotImplemented

    @property
    def internal_type(self):
        return 'function'

    def as_sql(self, backend):
        return NotImplemented
