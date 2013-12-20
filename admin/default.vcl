# This is a basic VCL configuration file for varnish.  See the vcl(7)
# man page for details on VCL syntax and semantics.
# 
# Default backend definition.  Set this to point to your content
# server.
# 
backend default {
    .host = "127.0.0.1";
    .port = "8080";
}
backend geoserver {
    .host = "172.31.18.207";
    .port = "8983";
}

sub vcl_fetch {
    set beresp.grace = 1h;
    unset beresp.http.Server;
    # These status codes should always pass through and never cache.
  if ( beresp.status >= 500 ) {
    set beresp.ttl = 0s;
  }
    if (beresp.http.content-type ~ "(text|javascript|json|xml|html)") {
        set beresp.do_gzip = true;
    }
  # CKAN cache headers are used by Varnish cache, but should not be propagated to
  # the Internet. Tell browsers and proxies not to cache. This means Varnish always
  # gets the responsibility to server the right content at all times.
  if (beresp.http.Cache-Control ~ "max-age") {
    unset beresp.http.set-cookie;
    set beresp.http.Cache-Control = "no-cache";
  }

  # Encourage assets to be cached by proxies and browsers
  # JS and CSS may be gzipped depending on headers
  # see https://developers.google.com/speed/docs/best-practices/caching
  if (req.url ~ "\.(css|js)") {
    set beresp.http.Vary = "Accept-Encoding";
  }

  # Encourage assets to be cached by proxies and browsers for 1 day
  if (req.url ~ "\.(png|gif|jpg|swf|css|js)") {
    unset beresp.http.set-cookie;
    set beresp.http.Cache-Control = "public, max-age=86400";
    set beresp.ttl = 1d;
  }

  # Encourage CKAN vendor assets (which are versioned) to be cached by
  # by proxies and browsers for 1 year
  if (req.url ~ "^/scripts/vendor/") {
    unset beresp.http.set-cookie;
    set beresp.http.Cache-Control = "public, max-age=31536000";
    set beresp.ttl = 12m;
  }
}
sub vcl_recv {
    if (req.http.user-agent ~ "Ezooms" || req.http.user-agent ~ "Ahrefs") {
	error 403;
    } 
if (req.url ~ "^/geoserver/") {
        set req.backend = geoserver;
    } else {
        set req.backend = default;
	#redirect secure traffic to https
	if ( (req.http.Cookie ~ "auth_tkt" || req.http.Cookie ~ "ckan" || req.url ~ "user/(reset|login)") && req.http.X-Forwarded-Proto !~ "(?i)https") {
		set req.http.x-Redir-Url = "https://data.gov.au" + req.url;
		error 753 req.http.x-Redir-Url;
	}
	# remove locale links
	if (req.url ~ "/((?!js)..|.._..|sr_Latn)/") {
	        set req.http.x-Redir-Url = regsub(req.url, "/((?!js)..|.._..|sr_Latn)/", "/");
		error 751 req.http.x-Redir-Url;
	}
	# rewrite broken resources
	if (req.url ~ "leaflet") {
	        set req.url = regsub(req.url, "fanstatic/ckanext-spatial/:version:2013-09-13T02:32:17.87/:bundle:js/vendor/leaflet/images", "js/vendor/leaflet/images");
	}
	# remove old hostnames
	if (req.http.host ~ "data.australia.gov.au") {
		set req.http.x-Redir-Url = "http://data.gov.au" + req.url;
		error 751 req.http.x-Redir-Url;
	}

	if (req.url ~ "^/_tracking") {
	// exclude web spiders from statistics
	    	if (req.http.user-agent ~ "Googlebot" || req.http.user-agent ~ "baidu" || req.http.user-agent ~ "bing") {
			error 200;
	    	} else {
			return (pass);
   		}
 	}
 if (req.url ~ "\.(png|gif|jpg|jpeg|swf|css|js|woff|eot)$") {
   //Varnish to deliver content from cache even if the request othervise indicates that the request should be passed
   return(lookup);
 }
}
  // Remove has_js and Google Analytics cookies. Evan added sharethis cookies
  set req.http.Cookie = regsuball(req.http.Cookie, "(^|;\s*)(__[a-z]+|has_js|cookie-agreed-en|_csoot|_csuid|_chartbeat2)=[^;]*", "");

  // Remove a ";" prefix, if present.
  set req.http.Cookie = regsub(req.http.Cookie, "^;\s*", "");
  // Remove empty cookies.
  if (req.http.Cookie ~ "^\s*$") {
    unset req.http.Cookie;
  }

  remove req.http.X-Forwarded-For;
  set req.http.X-Forwarded-For = req.http.X-Real-IP;
} 
sub vcl_hash {
     # http://serverfault.com/questions/112531/ignoring-get-parameters-in-varnish-vcl
     set req.url = regsub(req.url, "(?:(.com|.au))/((?!js)..|.._..|sr_Latn)/", "/");
     hash_data(req.url);
     if (req.http.host) {
         hash_data(req.http.host);
     } else {
         hash_data(server.ip);
     }
  if (req.http.Cookie) {
    hash_data(req.http.Cookie);
}
}
sub vcl_deliver {
    if (!resp.http.Vary) {
        set resp.http.Vary = "Accept-Encoding";   
    } else if (resp.http.Vary !~ "(?i)Accept-Encoding") {
        set resp.http.Vary = resp.http.Vary + ",Accept-Encoding";
    }    
    remove resp.http.X-Varnish;
    remove resp.http.Via;
    remove resp.http.Age;
    remove resp.http.X-Powered-By;
if (req.url ~ "^/geoserver/") {
  set resp.http.Access-Control-Allow-Origin = "*";
  set resp.http.Access-Control-Allow-Methods = "GET, POST, PUT, DELETE";
  set resp.http.Access-Control-Allow-Headers = "Origin, X-Requested-With, Content-Type, Accept";
}
}   
sub vcl_error {
    remove obj.http.Server;
	if (obj.status == 751) {
		set obj.http.Location = obj.response;
		set obj.status = 301;
		return (deliver);
	}
	if (obj.status == 753) {
		set obj.http.Location = obj.response;
		set obj.status = 301;
		return (deliver);
	}
}
# 
# Below is a commented-out copy of the default VCL logic.  If you
# redefine any of these subroutines, the built-in logic will be
# appended to your code.
# sub vcl_recv {
#     if (req.restarts == 0) {
# 	if (req.http.x-forwarded-for) {
# 	    set req.http.X-Forwarded-For =
# 		req.http.X-Forwarded-For + ", " + client.ip;
# 	} else {
# 	    set req.http.X-Forwarded-For = client.ip;
# 	}
#     }
#     if (req.request != "GET" &&
#       req.request != "HEAD" &&
#       req.request != "PUT" &&
#       req.request != "POST" &&
#       req.request != "TRACE" &&
#       req.request != "OPTIONS" &&
#       req.request != "DELETE") {
#         /* Non-RFC2616 or CONNECT which is weird. */
#         return (pipe);
#     }
#     if (req.request != "GET" && req.request != "HEAD") {
#         /* We only deal with GET and HEAD by default */
#         return (pass);
#     }
#     if (req.http.Authorization || req.http.Cookie) {
#         /* Not cacheable by default */
#         return (pass);
#     }
#     return (lookup);
# }
# 
# sub vcl_pipe {
#     # Note that only the first request to the backend will have
#     # X-Forwarded-For set.  If you use X-Forwarded-For and want to
#     # have it set for all requests, make sure to have:
#     # set bereq.http.connection = "close";
#     # here.  It is not set by default as it might break some broken web
#     # applications, like IIS with NTLM authentication.
#     return (pipe);
# }
# 
# sub vcl_pass {
#     return (pass);
# }
# 
# sub vcl_hash {
#     hash_data(req.url);
#     if (req.http.host) {
#         hash_data(req.http.host);
#     } else {
#         hash_data(server.ip);
#     }
#     return (hash);
# }
# 
# sub vcl_hit {
#     return (deliver);
# }
# 
# sub vcl_miss {
#     return (fetch);
# }
# 
# sub vcl_fetch {
#     if (beresp.ttl <= 0s ||
#         beresp.http.Set-Cookie ||
#         beresp.http.Vary == "*") {
# 		/*
# 		 * Mark as "Hit-For-Pass" for the next 2 minutes
# 		 */
# 		set beresp.ttl = 120 s;
# 		return (hit_for_pass);
#     }
#     return (deliver);
# }
# 
# sub vcl_deliver {
#     return (deliver);
# }
# 
# sub vcl_error {
#     set obj.http.Content-Type = "text/html; charset=utf-8";
#     set obj.http.Retry-After = "5";
#     synthetic {"
# <?xml version="1.0" encoding="utf-8"?>
# <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
#  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
# <html>
#   <head>
#     <title>"} + obj.status + " " + obj.response + {"</title>
#   </head>
#   <body>
#     <h1>Error "} + obj.status + " " + obj.response + {"</h1>
#     <p>"} + obj.response + {"</p>
#     <h3>Guru Meditation:</h3>
#     <p>XID: "} + req.xid + {"</p>
#     <hr>
#     <p>Varnish cache server</p>
#   </body>
# </html>
# "};
#     return (deliver);
# }
# 
# sub vcl_init {
# 	return (ok);
# }
# 
# sub vcl_fini {
# 	return (ok);
# }
