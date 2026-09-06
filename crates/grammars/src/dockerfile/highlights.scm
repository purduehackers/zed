; zed-extensions/dockerfile 812fbe0e227d06e2c9e17864cd96985b064125ef (MIT).
["FROM" "AS" "RUN" "CMD" "LABEL" "EXPOSE" "ENV" "ADD" "COPY"
 "ENTRYPOINT" "VOLUME" "USER" "WORKDIR" "ARG" "ONBUILD" "STOPSIGNAL"
 "HEALTHCHECK" "SHELL" "MAINTAINER" "CROSS_BUILD"
 (heredoc_marker) (heredoc_end)] @keyword
[":" "@"] @operator
(comment) @comment
[(double_quoted_string) (single_quoted_string) (json_string) (heredoc_line)] @string
(expansion ["$" "{" "}"] @punctuation.special) @none
((variable) @constant (#match? @constant "^[A-Z][A-Z_0-9]*$"))
