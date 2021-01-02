CREATE USER "monetdb_dfa" WITH PASSWORD 'monetdb' NAME 'MonetDB DfAnalyzer' SCHEMA "dataflow_analyzer";
GRANT SELECT ON dataflow_analyzer.ds_otrainingmodel TO monetdb_dfa;