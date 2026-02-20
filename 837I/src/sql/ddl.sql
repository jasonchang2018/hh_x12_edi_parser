create schema edwprodhh.edi_837i_parser;

create stage edwprodhh.edi_837i_parser.stg_response_iuhealth;
create stage edwprodhh.edi_837i_parser.stg_response_riverwood;

create table
    edwprodhh.edi_837i_parser.response
(
    response_id     varchar,
    pl_group        varchar,
    response_body   varchar,
    line_number     number,
    file_name       varchar,
    upload_date     date
)
;

create table
    edwprodhh.edi_837i_parser.response_files
(
    response_id     varchar,
    pl_group        varchar,
    file_name       varchar,
    upload_date     date
)
;

-- snowsql -q "PUT file://\\\\hh-fileserver01\\TempUL2\\IU_Health_Complex\\837_FILES_IN\\2026\\*_i*.837 @edwprodhh.edi_837i_parser.stg_response auto_compress=false;"
list @edwprodhh.edi_837i_parser.stg_response_iuhealth;

-- snowsql -q "file://C:\\Users\\jchang\\Desktop\\Projects\\incidentals\\2026-02-13-riverwoods-837\\SampleFiles\\SampleFiles\\837\\*.CLM @edwprodhh.edi_837i_parser.stg_response auto_compress=false;"
list @edwprodhh.edi_837i_parser.stg_response_riverwood;

create or replace file format
    edwprodhh.edi_837i_parser.format_txt
type                            = 'CSV'
field_delimiter                 = '\u0000'  -- an impossible delimiter; treats entire row as one value.
-- record_delimiter                = NONE      -- forces all lines as one value.
record_delimiter                = '\n'
skip_header                     = 0
field_optionally_enclosed_by    = NONE
escape_unenclosed_field         = NONE
;