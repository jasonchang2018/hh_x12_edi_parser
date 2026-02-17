create schema edwprodhh.edi_835_parser;

create stage edwprodhh.edi_835_parser.stg_response_iuhealth;
create stage edwprodhh.edi_835_parser.stg_response_riverwood;

create table
    edwprodhh.edi_835_parser.response
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
    edwprodhh.edi_835_parser.response_files
(
    response_id     varchar,
    pl_group        varchar,
    file_name       varchar,
    upload_date     date
)
;

-- snowsql -q "PUT file://\\\\hh-fileserver01\\TempUL2\\DATA_DIMENSIONS\\IN\\*.835 @edwprodhh.edi_835_parser.stg_response_iuhealth auto_compress=false;"
list @edwprodhh.edi_835_parser.stg_response_iuhealth;

-- snowsql -q "PUT file://C:\\Users\\jchang\\Desktop\\Projects\\incidentals\\2026-02-13-riverwoods-837\\SampleFiles\\SampleFiles\\835\\*.RMT @edwprodhh.edi_835_parser.stg_response_riverwood auto_compress=false;"
list @edwprodhh.edi_835_parser.stg_response_riverwood;

create or replace file format
    edwprodhh.edi_835_parser.format_txt
type                            = 'CSV'
field_delimiter                 = '\u0000'  -- an impossible delimiter; treats entire row as one value.
-- record_delimiter                = NONE      -- forces all lines as one value.
-- record_delimiter                = '\n'
record_delimiter                = '~'
skip_header                     = 0
field_optionally_enclosed_by    = NONE
escape_unenclosed_field         = NONE
;