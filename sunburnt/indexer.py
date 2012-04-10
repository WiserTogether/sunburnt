import datetime

from copy import deepcopy
from schema import get_attribute_or_callable


COMMIT_CHUNK_SIZE = 1000


class Field(object):
    def __init__(self, attribute=None, optional=False):
        self.attribute = attribute
        self.optional = optional
        self.solr_field = None


class IndexerMetaclass(type):
    """
    Metaclass that converts Field attributes to a dictionary called
    'base_fields', taking into account parent class 'base_fields' as well.
    """
    def __new__(cls, name, bases, attrs):
        super_new = super(IndexerMetaclass, cls).__new__

        new_class = super_new(cls, name, bases, {'__module__': attrs['__module__']})
        base_fields = getattr(new_class, 'base_fields', {})
        base_meta = getattr(new_class, '_meta', {})

        parents = [b for b in bases if isinstance(b, IndexerMetaclass)]
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return super_new(cls, name, bases, attrs)

        meta = base_meta
        for field_name, obj in attrs.items():
            if isinstance(obj, Field):
                base_fields.update({field_name: attrs.pop(field_name)})
            elif field_name == 'Meta':
                meta.update(dict((x, y) for x, y in obj.__dict__.items() if not x.startswith('__')))
                if 'type' not in meta:
                    raise Exception(
                        'Sunburnt indexer requires a type meta to be declared for grouping indices'
                    )

        if meta is None:
            raise Exception('You must provide Meta attributes for the `%s` indexer.' % name)

        # NOTE: We want to allow flexible meta parameters
        # elif set(meta.keys()).symmetric_difference(['type']):
        #    raise Exception('Invalid Meta parameters for `%s` indexer.' % name)

        attrs['base_fields'] = dict(base_fields)
        attrs['_meta'] = meta

        new_class = super_new(cls, name, bases, attrs)
        return new_class


class BaseIndexer(object):
    def __init__(self, interface):
        self.interface = interface
        self.index_timestamp = datetime.datetime.now()

        self.fields = deepcopy(self.base_fields)
        self.fields.update({
            'meta_type_s': Field(),
            'meta_index_timestamp_dt': Field(),
            })

        for field_name, field in self.fields.items():
            field.solr_field = self.interface.schema.match_field(field_name)

        self.interface.schema.check_fields(self.fields.keys())

    def transform(self, record):
        document = {}

        for field_name, field in self.fields.items():
            data = None
            try:
                if field.attribute is None:
                    if field.solr_field.dynamic:
                        value = getattr(
                            self,
                            'transform_%s' % field.solr_field.display_name(field_name)
                        )(record)
                        data = ((field_name, value),)
                    else:
                        value = getattr(self, 'transform_%s' % field_name)(record)
                        data = ((field_name, value),)
                else:
                    value = record
                    for name in field.attribute.split('.'):
                        if value is None:
                            raise AttributeError(
                                'Record: %s does not contain: %s currently trying to get: %s' % (
                                    record,
                                    field.attribute,
                                    name
                                ))
                        value = get_attribute_or_callable(value, name)
                    data = ((field_name, value),)
            except AttributeError:
                if not field.optional:
                    raise
            else:
                if data:
                    for name, value in data:
                        if value:
                            document[name] = value
        return document

    def transform_meta_type(self, record):
        return self._meta['type']

    def transform_meta_index_timestamp(self, record):
        return datetime.datetime.now()

    def get_records(self):
        raise NotImplementedError

    def add(self, records, commit=True):
        """
        If document ID exists in solr index, add functions as update
        """
        if not isinstance(records, (list, tuple)):
            records = [records]
        self.interface.add([self.transform(x) for x in records])
        if commit:
            self.interface.commit()

    def update(self, records, commit=True):
        self.add(records, commit=True)

    def delete(self, records, commit=True):
        if not isinstance(records, (list, tuple)):
            records = [records]
        self.interface.delete([self.transform_id(x) for x in records])
        if commit:
            self.interface.commit()

    def reindex(self):
        update_count = 0

        record_pool = []
        for record in self.get_records():
            if update_count % COMMIT_CHUNK_SIZE == 0:
                self.interface.add(record_pool)
                record_pool = []
            record_pool.append(self.transform(record))
            update_count += 1

        if len(record_pool):
            self.interface.add(record_pool)
            record_pool = []

        self.interface.commit()

        self.interface.delete(queries=(
            self.interface.Q(meta_type_s=self._meta['type']) &
            self.interface.Q(meta_index_timestamp_dt__lt=self.index_timestamp)
        ))

        return update_count


class Indexer(BaseIndexer):
    __metaclass__ = IndexerMetaclass
