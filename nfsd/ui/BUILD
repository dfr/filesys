#-
# Copyright (c) 2016 Doug Rabson
# All rights reserved.
#

genrule(
    name = "ui",
    srcs = glob([
        "*.html",
        "*.js",
        "*.css",
        "assets/*.svg",
        "components/*/*.html",
        "components/*/*.js",
        "deps/angular/angular.min.js",
        "deps/angular-route/angular-route.min.js",
        "deps/angular-resource/angular-resource.min.js",
        "deps/angular-animate/angular-animate.min.js",
        "deps/angular-aria/angular-aria.min.js",
        "deps/angular-messages/angular-messages.min.js",
        "deps/angular-material/angular-material.min.js",
        "deps/angular-material/angular-material.min.css",
    ]),
    outs = ["ui.h"],
    cmd = "python $(location static-content.py) registerUiContent $(SRCS) > $(OUTS)",
    tools = ["static-content.py"],
    visibility = ["//visibility:public"],
)
