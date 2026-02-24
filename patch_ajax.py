NEW_SCRIPT = """<script id="gtm-jq-ajax-listen" type="text/javascript">
(function() {
  'use strict';
  var $;
  var n = 0;
  init();

  function init(n) {
    if (typeof jQuery !== 'undefined') {
      $ = jQuery;
      bindToAjax();
    } else if (n < 20) {
      n++;
      setTimeout(init, 500);
    }
  }

  function bindToAjax() {
    $(document).bind('ajaxComplete', function(evt, jqXhr, opts) {
      var fullUrl = document.createElement('a');
      fullUrl.href = opts.url;
      var pathname = fullUrl.pathname[0] === '/' ? fullUrl.pathname : '/' + fullUrl.pathname;
      var queryString = fullUrl.search[0] === '?' ? fullUrl.search.slice(1) : fullUrl.search;
      var queryParameters = objMap(queryString, '&', '=', true);
      var headers = objMap(jqXhr.getAllResponseHeaders(), '\\n', ':');
      var responseBody = (jqXhr.responseJSON || jqXhr.responseXML || jqXhr.responseText || '');
      
      // AutoGTM Deep AJAX Parsing: Block false positives on 200 OK errors
      try {
          if (typeof responseBody === 'string') {
              var parsed = JSON.parse(responseBody);
              var strParsed = JSON.stringify(parsed).toLowerCase();
              // Standard failure indicators in JSON APIs
              if (strParsed.includes('"error"') || strParsed.includes('"status":0') || strParsed.includes('"false"')) {
                  console.warn("AutoGTM: Blocked ajaxComplete event due to detected payload error in response:", parsed);
                  return; // Abort dataLayer push
              }
          } else if (typeof responseBody === 'object') {
              var strObj = JSON.stringify(responseBody).toLowerCase();
              if (strObj.includes('"error":') || strObj.includes('"status":0') || strObj.includes('"success":false')) {
                  console.warn("AutoGTM: Blocked ajaxComplete event due to detected payload error in response object:", responseBody);
                  return; // Abort dataLayer push
              }
          }
      } catch(e) {
          // Not JSON, continue safely
      }

      dataLayer.push({
        'event': 'ajaxComplete',
        'attributes': {
          'type': opts.type || '',
          'url': fullUrl.href || '',
          'queryParameters': queryParameters,
          'pathname': pathname || '',
          'hostname': fullUrl.hostname || '',
          'protocol': fullUrl.protocol || '',
          'fragment': fullUrl.hash || '',
          'statusCode': jqXhr.status || '',
          'statusText': jqXhr.statusText || '',
          'headers': headers,
          'timestamp': evt.timeStamp || '',
          'contentType': opts.contentType || '',
          'response': responseBody
        }
      });
    });
  }

  function objMap(data, delim, spl, decode) {
    var obj = {};
    if (!data || !delim || !spl) { return {}; }
    var arr = data.split(delim);
    var i;
    for (i = 0; i < arr.length; i++) {
        var item = decode ? decodeURIComponent(arr[i]) : arr[i];
        var pair = item.split(spl);
        var key = trim_(pair[0]);
        var value = trim_(pair[1]);
        if (key && value) {
            obj[key] = value;
        }
    }
    return obj;
  }

  function trim_(str) {
    if (str) {
      return str.replace(/^[\\s\\uFEFF\\xA0]+|[\\s\\uFEFF\\xA0]+$/g, '');
    }
  }
})();
</script>"""

with open("main.py", "r") as f:
    content = f.read()

import re

# Find the block inside the 'value' key of the AJAX html tag and replace it
# It's a single massive line right now
pattern = r'("key": "html",\s*"value": )".*?(</script>)"'

def replacer(match):
    # JSON encode the multi-line script so it fits perfectly as a string value in python dictionary dump
    import json
    return match.group(1) + json.dumps(NEW_SCRIPT)

new_content = re.sub(pattern, replacer, content, count=1, flags=re.DOTALL)

with open("main.py", "w") as f:
    f.write(new_content)

print("Saved main.py with advanced AJAX listener")
