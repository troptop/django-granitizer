from .exceptions import GraphQLError
from django.core.exceptions import ObjectDoesNotExist,MultipleObjectsReturned
import graphene, sys

import logging
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
formatter = logging.Formatter('%(message)s - %(filename)s:%(lineno)s')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
""" filename = 'logs/item.log'
handlerFile = logging.FileHandler(filename)
handlerFile.setFormatter(formatter)
"""
handler.setFormatter(formatter)
logger.addHandler(handler)
#logger.addHandler(handlerFile)
class Granitizer(object):
    class Meta:
        graphene_class = None
        serializer_class = None
        relay_id_fields = {}
        nested_fields = {}
        model=None
        partial_update=True

    def __init__(self,info,input,
                implicit_filter={},
                implicit_payload={},
                partial_update = True,
                filter_key=None,
                payload_key=None,
                relay_id_fields=None,
                nested_fields=None,
                model=None,
                graphene_class=None,
                serializer_class=None,
    ):
      logger.info("Init Granitizer {}".format(__class__))

      default_relay_id_fields={}
      default_nested_fields={}
      default_filter_key=None
      default_payload_key=None
      default_partial_update=True
      default_model=None
      default_graphene_class=None
      default_serializer_class=None
      self._meta = self.Meta()
      self.graphene_class = self.set_initial_meta('graphene_class',default_graphene_class, graphene_class)
      self.serializer_class = self.set_initial_meta('serializer_class',default_serializer_class, serializer_class)
      self.relay_id_fields = self.set_initial_meta('relay_id_fields',default_relay_id_fields, relay_id_fields)
      self.partial_update = self.set_initial_meta('partial_update',default_partial_update, partial_update)
      self.filter_key = self.set_initial_meta('filter_key',default_filter_key, filter_key)
      self.payload_key = self.set_initial_meta('payload_key',default_payload_key, payload_key)
      self.nested_fields = self.set_initial_meta('nested_fields',default_nested_fields, nested_fields)
      self.granitized_payload = {}
      self.granitized_filter = {}
      self.info = info
      self.input = input
      self.initial_filter = self.get_data_from_input(self.filter_key)
      self.implicit_filter = implicit_filter
      self.global_filter = {**self.initial_filter, **self.implicit_filter}
      self.initial_payload = self.get_data_from_input(self.payload_key)
      self.implicit_payload = implicit_payload
      self.global_payload = {**self.initial_payload, **self.implicit_payload} 
      self.queryset = None
      self.serializer_object=None
      logger.info("Metadata of {}".format(self.__class__))
      logger.info(self._meta)

      logger.info('Checking "graphene_class" metadata :')
      if not self.graphene_class or (self.graphene_class and not hasattr(self.graphene_class,'_meta')):
          raise Exception(
              "graphene_class is required on all Granitize class "
          )
      logger.info("{} OK".format(self.graphene_class))
      logger.info('Checking "model" metadata :')
      if not hasattr(self.graphene_class._meta,'model') and not model:
        raise Exception(
        "_meta Model in graphene_class or _meta model missing"
      )
      self.model = getattr(self.graphene_class._meta,'model',model) 
      logger.info("{} OK".format(self.model))

      logger.info('Checking "fields" metadata of graphene class {} :'.format(self.graphene_class))
      if not hasattr(self.graphene_class._meta,'fields'):
        raise Exception("Granitize - fields is missing in _meta Node {}. Check your graphene Object (e.g. DjangoObjectType) - try fields = '__all__'".format(self.graphene_class))
      logger.info("{} OK".format(self.graphene_class._meta.fields))

      logger.info('Checking each "fields" metadata of graphene class {} defined in "relay_id_fields" or "nested_fields" attribute :'.format(self.graphene_class)) 
      logger.debug('relay_id_fields keys : {}'.format(self.relay_id_fields.keys()))
      logger.debug('nested_fields keys : {}'.format(self.nested_fields.keys()))
      for key, value in self.graphene_class._meta.fields.items():
        logger.debug("{} - {}".format(key,value))
        logger.info(type(value))
        logger.info(isinstance(value,graphene.relay.node.GlobalID))
        logger.info(isinstance(value,graphene.Dynamic))
        if type(value) in (graphene.relay.node.GlobalID,graphene.Dynamic):
          logger.info("I am in")
          if not (key in self.relay_id_fields.keys() or key in self.nested_fields.keys() or callable(getattr(self, 'granitize_field_' + key,None))):
              raise Exception('Granitize - Error with Field "{}" in class "{}". You should add this field to the Meta "relay_id_fields" or "nested_fields" or define the function "{}" - field type : {}'.format(key,self.__class__,'granitize_field_' + key,value) )
      logger.info("Attributes Checks OK")
      t = not (key in self.relay_id_fields.keys() or key in self.nested_fields.keys() or callable(getattr(self, 'granitize_field_' + key,None)))
      logger.info(t)

      ## Parsing/Granitize filter
      logger.info("Initial Filter Parsing")
      if self.initial_filter:
        logger.info("initial_filter exist")
        for field_name,field_value in self.global_filter.items():
          logger.info("{} - {}".format(field_name,field_value))
          if callable(getattr(self.__class__, 'granitize_filter_field_' + field_name,None)):
            logger.info("call {}.{}".format(self.__class__,'granitize_filter_field_' + field_name))
            granitize_value = getattr(self.__class__, 'granitize_filter_field_' + field_name)(self,field_name,field_value) 

          elif callable(getattr(self.__class__, 'granitize_field_' + field_name,None)):
            logger.info("call {}.{}".format(self.__class__,'granitize_field_' + field_name))
            granitize_value = getattr(self.__class__, 'granitize_field_' + field_name)(self,field_name,field_value) 
          else:
            logger.info("call {}.{}".format(self.__class__,'granitize_fields'))
            granitize_value = self.granitize_fields(field_name,field_value)
          
          logger.info("convert : {} - {}".format(field_name,granitize_value))
          self.granitized_filter[field_name] = granitize_value
        logger.info("Initial Filter Parsed Result : ")
        logger.info("{}".format(self.granitized_filter))

        logger.info("Generating QuerySet ")
        self.queryset = self.get_queryset(self.model, self.granitized_filter)
        if self.queryset == None:
            raise GraphQLError(
                "queryset should not be None if initial_filter initialized"
            )
        logger.info("QUERYSET ____ QuerySet Generated : ")
        logger.info("{}".format(self.queryset))

      logger.info("Initial Payload Parsing")
      """ Parsing/Granitize payload data """
      for field_name,field_value in self.global_payload.items():
        if callable(getattr(self.__class__, 'granitize_payload_field_' + field_name,None)):
          logger.info("call {}.{}".format(self.__class__,'granitize_payload_field_'))
          granitize_value = getattr(self.__class__, 'granitize_payload_field_' + field_name)(self,field_name,field_value) 
        elif callable(getattr(self.__class__, 'granitize_field_' + field_name,None)):
          logger.info("call {}.{}".format(self.__class__,'granitize_field_'))
          granitize_value = getattr(self.__class__, 'granitize_field_' + field_name)(self,field_name,field_value) 
        else:
          logger.info("call {}.{}".format(self.__class__,'granitize_fields'))
          granitize_value = self.granitize_fields(field_name,field_value)
          logger.info("call {}.{}".format(self.__class__,'granitize_nested_fields'))
          granitize_value = self.granitize_nested_fields(field_name,field_value)
          logger.info("convert : {} - {}".format(field_name,granitize_value))

        self.granitized_payload[field_name] = granitize_value   
        logger.info("PAYLOAD ________ Initial Payload Parsed Result : ")
        logger.info("{}".format(self.granitized_payload))
      logger.info("{}.{}".format(self.__class__,'save()'))

    def granitize_nested_fields(self,field_name,field_value):
      if field_name in self.nested_fields.keys():
        return self.mutate_nested_fields(field_name,field_value)
      return field_value

    def mutate_nested_fields(self, field_name,field_value ):
      granitize_class = self.nested_fields[field_name]
      if isinstance(field_value,list):
        nodes=[]
        for each_field_value in field_value:
            node = granitize_class(self.info,each_field_value,
                self.implicit_filter,
                self.implicit_payload,
                self.partial_update,
                self.filter_key,
                self.payload_key,
                self.relay_id_fields,
                self.nested_fields,
                self.model,
                self.graphene_class,
                self.serializer_class).save()
            if not node:
                raise GraphQLError(
                    "Node not found from input field : {}".format({field_name:each_field_value})
                )
            nodes.append(node)
        return nodes                    
      else:
        node = granitize_class(self.info,field_value,
                  self.implicit_filter,
                  self.implicit_payload,
                  self.partial_update,
                  self.filter_key,
                  self.payload_key,
                  self.relay_id_fields,
                  self.nested_fields,
                  self.model,
                  self.graphene_class,
                  self.serializer_class).save()
        if not node:
          raise GraphQLError(
              "Node not found from input field : {}".format({field_name:field_value})
          )
        return node


    def save(self):
      s = self.build_serializer()
      logger.info("SAVE _____")
      logger.info(s)
      logger.info(s.initial_data)
      
      if s.is_valid(raise_exception=True):
        save_instance = s.save()
      return save_instance

    def build_serializer(self):
      if self.queryset:
          serializer_instance = self.serializer_class(self.queryset,partial=self.partial_update, data=self.granitized_payload)
      else:
          serializer_instance = self.serializer_class(data=self.granitized_payload)
      if serializer_instance == None:
          raise GraphQLError(
              "serializer_object should not be None if initial_payload initialized"
          )
      return serializer_instance
  
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

    def get_filter_from_input(self,keywords=None):
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
