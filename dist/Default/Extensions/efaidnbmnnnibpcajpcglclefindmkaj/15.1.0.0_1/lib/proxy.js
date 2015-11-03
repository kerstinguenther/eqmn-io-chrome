/*************************************************************************
* ADOBE CONFIDENTIAL
* ___________________
*
*  Copyright 2015 Adobe Systems Incorporated
*  All Rights Reserved.
*
* NOTICE:  All information contained herein is, and remains
* the property of Adobe Systems Incorporated and its suppliers,
* if any.  The intellectual and technical concepts contained
* herein are proprietary to Adobe Systems Incorporated and its
* suppliers and are protected by all applicable intellectual property laws,
* including trade secret and or copyright laws.
* Dissemination of this information or reproduction of this material
* is strictly forbidden unless prior written permission is obtained
* from Adobe Systems Incorporated.
**************************************************************************/

/*global chrome, console, require:true, exports:true, define:true, $, def:true */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

function dependOn() {
    'use strict';
    return [
        require("util")
    ];
}

var def;
require = function (m) {'use strict'; return m; };
if (window.define) {
    def = window.define;
} else {
    def = function (deps, fn) {
        'use strict';
        return fn.apply(null, [{
            ajax: $.ajax.bind($)
        }]);
    };
}
var exports = acom_analytics = {};

def(dependOn(), function (util) {
    'use strict';

    function errnum() {
        return Math.ceil(Math.random() * 1000000);
    }

    function reduceStackTrace(stack) {
        var lines = stack.split(util.stackDelimiter()),
            i,
            line,
            output = [],
            len = 0;

        for (i = 0; i < lines.length; i += 1) {
            line = lines[i];
            if (line.indexOf("run_function") === -1 && len < 1000) {
                // strip extraneous chrome prefix stuff
                line = line.replace(/chrome\-extension:\/\/[a-zA-Z]*\//g, "");
                // strip extraneous FF prefix stuff
                line = line.replace(/jar:file:\S*\.xpi!/, "");
                line = line.replace(/resource:\S+toolkit/, "");
                line = line.replace(/resource:\S+jetpack/, "");
                len += line.length;
                output.push(line);
            }
        }
        return output.join("\n");
    }
    var error_handlers = [],

        run_function = function () {
            try {
                // call the function with any parameters provided at the
                // declaration, combined with any parameters provided
                // here
                return this.func.apply(
                    this.context,
                    this.args.concat(
                        Array.prototype.slice.call(arguments, 0)
                    )
                );
            } catch (e) {
                if (!e.handled) {
                    e.handled = true;
                    var params = {
                            errnum: errnum(),
                            name: e.name + (e.message ? " " + e.message : ""),
                            source: "client",
                            details: ""
                        },
                        // 2048 is max allowable message lenght.  128 buffer
                        len = 2048 - (JSON.stringify(params).length + 128);

                    params.details = reduceStackTrace(e.stack).substr(0, len);
                    util.each(error_handlers, function (index, f) {
                        f(params, 40, e);
                    });
                    this.context.LOG(params, 40);
                }
                // re-throw so that normal processing is interupted.
                throw e;
            }

        },

        proxy = exports.proxy = function (func) {
            return run_function.bind(
                {
                    func: func,
                    context: this,
                    // store any additional parameters...
                    args: Array.prototype.slice.call(arguments, 1)
                }
            );
        },

        handlers = exports.handlers = function (func) {
            error_handlers.push(func);
        },

        REST_error = exports.REST_error = function (xhr, context, extras) {
            var params = {
                    errnum: errnum(),
                    name: xhr.statusText,
                    status: xhr.status,
                    details: "HTTP error"
                };

            if (extras) {
                params = util.extend(params, extras);
            }

            if (xhr.responseJSON) {
                params.name = xhr.responseJSON.error.code;
                params.details = xhr.responseJSON.error.message;
            }
            util.each(error_handlers, function (index, f) {
                f(params, 40);
            });
            context.LOG(params, 40);
        };


    return {proxy: proxy, REST_error: REST_error, handlers: handlers};
});
