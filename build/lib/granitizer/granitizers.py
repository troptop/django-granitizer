from .exceptions import GraphQLError
from django.core.exceptions import ObjectDoesNotExist,MultipleObjectsReturned
import graphene

import logging
logger = logging.getLogger(__name__)

class Granitizer(object):
    class Meta:
        graphene_class = None
        serializer_class = None
        relay_id_fields = {}
        nested_fields = {}
        model=None

    def __init__(self,info,input,
                implicit_filter={},
                implicit_payload={},
                filter_key=None,
                payload_key=None,
                relay_id_fields=None,
                #nested_fields=None,
                model=None,
                graphene_class=None,
                serializer_class=None,
    ):
        
        default_relay_id_fields={}
        #default_nested_fields={}
        default_filter_key=None
        default_payload_key=None
        default_model=None
        default_graphene_class=None
        default_serializer_class=None
        self._meta = self.Meta()
        self.graphene_class = self.set_initial_meta('graphene_class',default_graphene_class, graphene_class)
        self.serializer_class = self.set_initial_meta('serializer_class',default_serializer_class, serializer_class)
        self.relay_id_fields = self.set_initial_meta('relay_id_fields',default_relay_id_fields, relay_id_fields)
        self.filter_key = self.set_initial_meta('filter_key',default_filter_key, filter_key)
        self.payload_key = self.set_initial_meta('payload_key',default_payload_key, payload_key)

        self.granitized_payload = {}
        self.granitized_filter = {}
        self.info = info
        self.input = input
        
        self.initial_filter = self.get_data_from_input(self.filter_key)
        self.implicit_filter = implicit_filter
        self.global_filter = {**self.initial_filter, **self.implicit_filter}
        self.initial_payload = self.get_data_from_input(self.payload_key)
        if not self.initial_payload:
            raise Exception(
                "No payload in the request"
            )
        self.implicit_payload = implicit_payload
        self.global_payload = {**self.initial_payload, **self.implicit_payload} 
        self.queryset = None
        self.serializer_object=None
        """ self.nested_granitizer_objects=[]
        self.nested_fields = self.set_initial_meta('nested_fields',default_nested_fields, nested_fields)
        self.granitized_payload_with_nested={}
        self.granitized_filter_with_nested={} """
        if not self.graphene_class or (self.graphene_class and not hasattr(self.graphene_class,'_meta')):
            raise Exception(
                "graphene_class is required on all Granitize class "
            )
        if not hasattr(self.graphene_class._meta,'model') and not model:
            raise Exception(
            "_meta Model in graphene_class or _meta model missing"
        )
        if not model:
            self.model = getattr(self.graphene_class._meta,'model') 
        else:
            self.model = model

        if not hasattr(self.graphene_class._meta,'fields'):
            raise Exception("Granitize - fields is missing in _meta Node {}. Check your graphene Object (e.g. DjangoObjectType) - try fields = '__all__'".format(granitize_node))
        for key, value in self.graphene_class._meta.fields.items():
            if value in (graphene.relay.node.GlobalID,graphene.Dynamic):
                if not (key in self.relay_id_fields.keys() or key in self.nested_fields.keys() or callable(getattr(self, 'granitize_field_' + key,None))):
                    raise Exception('Granitize - Field {} is missing in Meta "IDs" or define the function "{}" - field type : {}'.format(key,'granitize_field_' + key,value) )

        """ Parsing/Granitize payload data """
        for field_name,field_value in self.global_payload.items():
            if callable(getattr(self.__class__, 'granitize_payload_field_' + field_name,None)):
                granitize_value = getattr(self.__class__, 'granitize_payload_field_' + field_name)(self,field_name,field_value) 
            elif callable(getattr(self.__class__, 'granitize_field_' + field_name,None)):
                granitize_value = getattr(self.__class__, 'granitize_field_' + field_name)(self,field_name,field_value) 
            else:
                granitize_value = self.granitize_fields(field_name,field_value)
            self.granitized_payload[field_name] = granitize_value
        #self.granitized_payload_with_nested = self.build_payload(self.granitized_payload)
        if self.initial_filter:
            for field_name,field_value in self.global_filter.items():
                if callable(getattr(self.__class__, 'granitize_filter_field_' + field_name,None)):
                    granitize_value = getattr(self.__class__, 'granitize_filter_field_' + field_name)(self,field_name,field_value) 
                elif callable(getattr(self.__class__, 'granitize_field_' + field_name,None)):
                    granitize_value = getattr(self.__class__, 'granitize_field_' + field_name)(self,field_name,field_value) 
                else:
                    granitize_value = self.granitize_fields(field_name,field_value)
                #self.granitized_filter_with_nested[field_name] = self.granitize_nested_fields(field_name, field_value)
                self.granitized_filter[field_name] = granitize_value
            #self.granitized_filter_with_nested = self.build_filter(self.granitized_filter)
            #self.build_queryset(self.model,self.granitized_filter_with_nested)
            self.queryset = self.get_queryset(self.model, self.granitized_filter)
            if self.queryset == None:
                raise GraphQLError(
                    "queryset should not be None if initial_filter initialized"
                )
        if self.initial_payload:
            self.serializer_object = self.build_serializer(self.serializer_class,self.queryset,self.granitized_payload)

    def set_initial_meta(self, name, default_value, arg_value):
            meta = default_value
            if hasattr(self._meta,name) :
                meta = getattr(self._meta,name)
            if arg_value is not None:
                meta = arg_value
            return meta
    
    def get_data_from_input(self,keywords=None):
        dic = {}
        if not keywords:
            raise GraphQLError(
            "parameters missing in payloads_key or filters_key"
        )
        if 'uniq' in keywords:
            for uniq in keywords['uniq']:
                if 'key' in uniq and uniq['key'] and uniq['key'] in self.input:
                    dic[uniq['key']]=self.input[uniq['key']]
                    return dic
        if 'dict' in keywords:
            for dico in keywords['dict']:
                if 'key' in dico and dico['key'] and dico['key'] in self.input:
                    dic[dico['key']]=self.input[dico['key']]
                    return dic[dico['key']]
        return dic



    def get_queryset(self,model, filter ):
        try:
            queryset = model.objects.get(**filter)
        except MultipleObjectsReturned:
            raise GraphQLError(
                "Filter return more than one object - filter : {}".format(filter)
            )
        except ObjectDoesNotExist:
            raise GraphQLError(
                "no object found - filter : {}".format(filter)
            )
            queryset = None
        return queryset

    def build_serializer(self,serializer_class,queryset, payload):
            if queryset:
                serializer_instance = serializer_class(queryset,partial=True, data=payload)
            else:
                serializer_instance = serializer_class(data=payload)
            if serializer_instance == None:
                raise GraphQLError(
                    "serializer_object should not be None if initial_payload initialized"
                )
            return serializer_instance
    def granitize_mutation(self, raise_exception=True):
        if self.serializer_object.is_valid(raise_exception=raise_exception):
            obj_saved = self.serializer_object.save()
            if obj_saved:
                return obj_saved
            else:
                raise GraphQLError(
                    "Serializer object save error : {}".format(self.__class__)
                )

    def get_object_from_global_id(self, field_name,field_value,node_type):
        node = graphene.Node.get_node_from_global_id(self.info,field_value,node_type)
        return node
        

    def get_object_pk_from_global_id(self, field_name,field_value,node_type):
        node_id = getattr(self.get_object_from_global_id(field_name,field_value,node_type),'pk',None)
        return node_id

    def granitize_fields(self,field_name,field_value):        
        if field_name in self.relay_id_fields.keys():
            return self.granitize_relay_id_fields(field_name,field_value)
        return field_value


    def granitize_relay_id_fields(self,field_name,field_value):
        if field_name in self.relay_id_fields.keys():
            if isinstance(field_value,list):
                nodes=[]
                for each_field_value in field_value:
                    node = self.get_object_pk_from_global_id(field_name,each_field_value, self.relay_id_fields[field_name]) 
                    if not node:
                        raise GraphQLError(
                            "Node not found from input field : {}".format({field_name:each_field_value})
                        )
                    nodes.append(node)
                return nodes                    
            else:
                node = self.get_object_pk_from_global_id(field_name,field_value, self.relay_id_fields[field_name])
            if not node:
                raise GraphQLError(
                    "Node not found from input field : {}".format({field_name:field_value})
                )
            return node
        return field_value

    """ def get_filter_from_input(self):
        keys = dict(self.input)
        keys.pop('data',None)
        return keys
    
    def get_payload_from_input(self):
        keys = dict(self.input)
        data = keys.pop('data',None)
        return data """

    """ def mutate(self):
        if not self.granitized_filter_with_nested:
            for field_name,field_value in granitized_payload_with_nested.items():
                if field_name in self.nested_fields.keys():
                    if isinstance(field_value,list):
                        obj_list = []
                        for each in field_value:
                            each.serializer_object.is_valid(raise_exception=True)
                            obj_list.append(each.serializer_object.save())
                        self.granitized_payload_with_nested[field_name]=obj_list
                    else:
                        field_value.serializer_object.is_valid(raise_exception=True)
                        self.granitized_payload_with_nested[field_name]=field_value.serializer_object.save()
            self.serializer_object.is_valid(raise_exception=True)
            return self.serializer_object.save()

    def build_payload(self,granitized_payload):
        granitized_payload_with_nested = []
        for field_name,field_value in granitized_payload.items():
            granitized_payload_with_nested[field_name] = self.granitize_nested_fields(field_name, field_value)
        return granitized_payload_with_nested

    def build_filter(self,granitized_filter):
        granitized_filter_with_nested=[]
        for field_name,field_value in granitized_filter.items():
            value_or_g_instance = self.granitize_nested_fields(field_name, field_value)
            if field_name in self.nested_fields.keys():
                granitized_filter_with_nested[field_name] = value_or_g_instance.queryset
            else:
                granitized_filter_with_nested[field_name] = value_or_g_instance
        return granitized_filter_with_nested """

    """ def granitize_nested_fields(self,field_name,field_value):
        if field_name in self.nested_fields.keys():
            if isinstance(self.nested_fields[field_name],list):
                granitize_class = self.nested_fields[field_name][0]
                if not isinstance(field_value,list):
                    raise GraphQLError(
                        "Field value {} should be a List ".format(field_name)
                    )
                g_instance = []
                for each_field_value in field_value:
                    g_instance.append(granitize_class(self.info,each_field_value))
            else:
                granitize_class = self.nested_fields[field_name]
                g_instance = granitize_class(self.info,field_value)
            return g_instance
        return field_value """

    """ def granitize_mutation(self,raise_exception=True):
        for field_name in self.nested_fields.keys():
            if isinstance(self.nested_fields[field_name],list):
                self.granitized_payload[field_name] = []
            else:
                self.granitized_payload[field_name] = None

        for nested_granitizer_object in self.nested_granitizer_objects:
            field_name = nested_granitizer_object["field_name"]
            if isinstance(self.nested_fields[field_name],list):
                self.granitized_payload[field_name].append(getattr(nested_granitizer_object['granitizer_instance'].granitize_mutation(raise_exception=raise_exception),'pk',None))
            else:
                self.granitized_payload[field_name] = getattr(nested_granitizer_object['granitizer_instance'].granitize_mutation(raise_exception=raise_exception),'pk',None)


        if self.serializer_object.is_valid(raise_exception=raise_exception):
            obj_saved = self.serializer_object.save()
            if obj_saved:
                return obj_saved
            else:
                raise GraphQLError(
                    "Serializer object save error : {}".format(self.__class__)
                ) """

