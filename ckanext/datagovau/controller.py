import urllib
import json
from pprint import pprint
import logging
import ckan.logic as logic
import hashlib
import threading
from ckan.common import _, c, request, response
from pylons import config

log = logging.getLogger(__name__)

from ckan.controllers.api import ApiController

class DGAApiController(ApiController):

	def _post_analytics(self,user,request_obj_type,request_function,request_id):
		if (config.get('googleanalytics.id') != None):
			data = urllib.urlencode({
				"v":1, 
				"tid":config.get('googleanalytics.id'), 
				"cid":hashlib.md5(user).hexdigest(), 
				"t":"event", 
				"dh":c.environ['HTTP_HOST'], 
				"dp":c.environ['PATH_INFO'], 
				"dr":c.environ.get('HTTP_REFERER',''), 
				"ec":"CKAN API Request", 
				"ea":request_obj_type+request_function, 
				"el":request_id, 
				})
			log.debug("Sending API Analytics Data: "+data)
			# send analytics asynchronously
	 		threading.Thread(target=urllib.urlopen,args=("http://www.google-analytics.com/collect", data)).start()
		

	def action(self, logic_function, ver=None):
		try:
	            function = logic.get_action(logic_function)
                except Exception,e:
		    log.debug(e)
                    pass
		try:
	            side_effect_free = getattr(function, 'side_effect_free', False)
	            request_data = self._get_request_data(try_url_params=side_effect_free)
		    if isinstance(request_data, dict):
			id = request_data.get('id','')
                        if 'q' in request_data.keys():
                                id = request_data['q']
                        if 'query' in request_data.keys():
                                id = request_data['query']
			self._post_analytics(c.user,logic_function,'', id)
	        except Exception,e:
		    print log.debug(e)
	      	    pass
	
		return ApiController.action(self,logic_function, ver)
		
	def list(self, ver=None, register=None, subregister=None, id=None):
		self._post_analytics(c.user,register+("_"+str(subregister) if subregister else ""),"list",id)
		return ApiController.list(self,ver, register, subregister, id)
	def show(self, ver=None, register=None, subregister=None, id=None, id2=None):
		self._post_analytics(c.user,register+("_"+str(subregister) if subregister else ""),"show",id)
		return ApiController.show(self,ver, register, subregister, id,id2)
	def update(self, ver=None, register=None, subregister=None, id=None, id2=None):
		self._post_analytics(c.user,register+("_"+str(subregister) if subregister else ""),"update",id)
		return ApiController.update(self,ver, register, subregister, id,id2)
	def delete(self, ver=None, register=None, subregister=None, id=None, id2=None):
		self._post_analytics(c.user,register+("_"+str(subregister) if subregister else ""),"delete",id)
		return ApiController.delete(self,ver, register, subregister, id,id2)
	def search(self, ver=None, register=None):
		id = None
		try:
        	        params = MultiDict(self._get_search_params(request.params))
			if 'q' in params.keys():
				id = params['q']
			if 'query' in params.keys():
				id = params['query']
            	except ValueError, e:
			print str(e)
			pass
		self._post_analytics(c.user,register,"search",id)
