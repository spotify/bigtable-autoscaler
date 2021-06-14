CREATE TYPE error_code AS ENUM(
  'OK',
  'GRPC_CANCELLED',
  'GRPC_UNKNOWN',
  'GRPC_INVALID_ARGUMENT',
  'GRPC_DEADLINE_EXCEEDED',
  'GRPC_NOT_FOUND',
  'GRPC_ALREADY_EXISTS',
  'GRPC_PERMISSION_DENIED',
  'GRPC_RESOURCE_EXHAUSTED',
  'GRPC_FAILED_PRECONDITION',
  'GRPC_ABORTED',
  'GRPC_OUT_OF_RANGE',
  'GRPC_UNIMPLEMENTED',
  'GRPC_INTERNAL',
  'GRPC_UNAVAILABLE',
  'GRPC_DATA_LOSS',
  'GRPC_UNAUTHENTICATED',
  'PROJECT_NOT_FOUND',
  'AUTOSCALER_INTERNAL'
);


CREATE TABLE IF NOT EXISTS autoscale (
    project_id character varying(256) NOT NULL,
    instance_id character varying(256) NOT NULL,
    cluster_id character varying(256) NOT NULL,
    min_nodes integer NOT NULL,
    max_nodes integer NOT NULL,
    cpu_target double precision NOT NULL,
    storage_target double precision NOT NULL default(0.7),
    overload_step integer,
    last_change timestamp with time zone,
    last_check timestamp with time zone,
    enabled boolean default(true),
    last_failure timestamp with time zone,
    consecutive_failure_count int default(0),
    last_failure_message text,
    min_nodes_override integer NOT NULL default(0),
    error_code error_code NOT NULL default('OK'),
    extra_enabled_algorithms character varying(256),
    CONSTRAINT full_name PRIMARY KEY(project_id, instance_id, cluster_id),
    CONSTRAINT autoscale_cpu_target_check CHECK ((cpu_target > (0.0)::double precision)),
    CONSTRAINT autoscale_cpu_target_check1 CHECK ((cpu_target < (1.0)::double precision)),
    CONSTRAINT autoscale_min_nodes_check CHECK ((min_nodes >= 1)),
    CONSTRAINT autoscale_overload_step_check1 CHECK (((overload_step > 0) OR (overload_step IS NULL))),
    CONSTRAINT autoscale_max_nodes_check CHECK(max_nodes >= min_nodes),
    CONSTRAINT min_nodes_override_check CHECK(min_nodes_override >= 0),
    CONSTRAINT autoscale_storage_target_check CHECK ((storage_target > (0.0)::double precision)),
    CONSTRAINT autoscale_storage_target_check1 CHECK ((storage_target < (1.0)::double precision))
);


CREATE TABLE IF NOT EXISTS resize_log (
    timestamp timestamp with time zone,
    project_id character varying(256) NOT NULL,
    instance_id character varying(256) NOT NULL,
    cluster_id character varying(256) NOT NULL,
    min_nodes integer NOT NULL,
    max_nodes integer NOT NULL,
    cpu_target double precision NOT NULL,
    overload_step integer,
    current_nodes integer NOT NULL,
    target_nodes integer NOT NULL,
    cpu_utilization double precision NOT NULL,
    storage_target double precision NOT NULL default(0.7),
    storage_utilization double precision NOT NULL,
    detail text,
    success boolean,
    error_message text,
    min_nodes_override integer NOT NULL default(0)
);

CREATE INDEX ON resize_log(timestamp);

