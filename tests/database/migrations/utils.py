import datetime

EMPTY_MIGRATION = {
    'id': '79f47320e4',
    'date': datetime.datetime.now(tz=datetime.UTC).isoformat(),
    'number': 1,
    'migrated': False,
    'in_memory': False,
    'schema': {}
}


EMPTY_MIGRATED_SCHEMA = {
    'id': '79f47320e4',
    'date': datetime.datetime.now(tz=datetime.UTC).isoformat(),
    'number': 1,
    'migrated': True,
    'in_memory': False,
    'schema': {}
}


COMPLETE_MIGRATED_SCHEMA = {
    'id': '79f47320e4',
    'date': datetime.datetime.now(tz=datetime.UTC).isoformat(),
    'number': 1,
    'migrated': True,
    'in_memory': False,
    'schema': {
        'name': 'companies',
        'tables': [
            {
                'name': 'company',
                'fields': [
                    [
                        'CharField',
                        'name',
                        {
                            'null': False,
                            'primary_key': False,
                            'default': None,
                            'unique': False,
                            'editable': False,
                            'max_length': 5
                        }
                    ],
                    [
                        'AutoField',
                        'id',
                        {
                            'null': False,
                            'primary_key': True,
                            'default': None,
                            'unique': False,
                            'editable': False
                        }
                    ]
                ],
                'indexes': [],
                'constraints': [],
                'ordering': [],
                'str_field': 'id'
            }
        ]
    }
}
