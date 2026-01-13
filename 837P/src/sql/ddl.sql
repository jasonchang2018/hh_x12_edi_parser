create schema edwprodhh.edi_837p_parser;

create stage
    edwprodhh.edi_837p_parser.stg_response
;

create table
    edwprodhh.edi_837p_parser.response
(
    response_id     varchar,
    response_body   varchar,
    line_number     number,
    file_name       varchar,
    upload_date     date
)
;

create table
    edwprodhh.edi_837p_parser.response_files
(
    file_name       varchar
)
;

-- snowsql -q "PUT file://\\\\hh-fileserver01\\TempUL2\\IU_Health_Complex\\837_FILES_IN\\2026\\*_p*.837 @edwprodhh.edi_837p_parser.stg_response auto_compress=false;"
-- list @edwprodhh.edi_837p_parser.stg_response;

create or replace file format
    edwprodhh.edi_837p_parser.format_txt
type                            = 'CSV'
field_delimiter                 = '\u0000'  -- an impossible delimiter; treats entire row as one value.
-- record_delimiter                = NONE      -- forces all lines as one value.
record_delimiter                = '\n'
skip_header                     = 0
field_optionally_enclosed_by    = NONE
escape_unenclosed_field         = NONE
;