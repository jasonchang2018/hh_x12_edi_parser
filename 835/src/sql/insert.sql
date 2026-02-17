create or replace procedure
    edwprodhh.edi_835_parser.insert_835_from_stage(EXECUTE_TIME TIMESTAMP_LTZ(9))
returns     boolean
language    sql
as
begin


    insert into
        edwprodhh.edi_835_parser.response
    (
        RESPONSE_ID,
        RESPONSE_BODY,
        LINE_NUMBER,
        FILE_NAME,
        UPLOAD_DATE,
        PL_GROUP
    )
    select      sha2(METADATA$FILENAME)                                             as response_id,
                $1                                                                  as response_body,
                row_number() over (partition by METADATA$FILENAME order by seq)     as line_number,
                METADATA$FILENAME                                                   as file_name,
                :execute_time::date                                                 as upload_date,
                'IU HEALTH - TPL'                                                   as pl_group
    from        (
                    select      $1,
                                metadata$filename,
                                METADATA$FILE_ROW_NUMBER as seq
                    from        @edwprodhh.edi_835_parser.stg_response_iuhealth
                                (file_format => edwprodhh.edi_835_parser.format_txt)
                    where       METADATA$FILENAME not in (select file_name from edwprodhh.edi_835_parser.response_files)
                )
    ;

    

    insert into
        edwprodhh.edi_835_parser.response
    (
        RESPONSE_ID,
        RESPONSE_BODY,
        LINE_NUMBER,
        FILE_NAME,
        UPLOAD_DATE,
        PL_GROUP
    )
    select      sha2(METADATA$FILENAME)                                             as response_id,
                $1                                                                  as response_body,
                row_number() over (partition by METADATA$FILENAME order by seq)     as line_number,
                METADATA$FILENAME                                                   as file_name,
                :execute_time::date                                                 as upload_date,
                'RIVERWOOD HEALTH - TPL'                                            as pl_group
    from        (
                    select      $1,
                                metadata$filename,
                                METADATA$FILE_ROW_NUMBER as seq
                    from        @edwprodhh.edi_835_parser.stg_response_riverwood
                                (file_format => edwprodhh.edi_835_parser.format_txt)
                    where       METADATA$FILENAME not in (select file_name from edwprodhh.edi_835_parser.response_files)
                )
    ;

    insert into
        edwprodhh.edi_835_parser.response_files
    (
        response_id,
        pl_group,
        file_name,
        upload_date
    )
    select      distinct
                response_id,
                pl_group,
                file_name,
                upload_date
    from        edwprodhh.edi_835_parser.response
    where       upload_date = current_date()
    ;

end
;



create or replace task
    edwprodhh.edi_835_parser.sp_insert_835_from_stage
    warehouse = analysis_wh
    schedule = 'USING CRON 0 1 * * * America/Chicago'
as
call    edwprodhh.edi_835_parser.insert_835_from_stage(current_timestamp())
;