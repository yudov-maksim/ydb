PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

PEERDIR(
    library/cpp/lwtrace/protos
)

SRCS(
    error.proto
)

CPP_PROTO_PLUGIN0(validation ydb/public/lib/validation)

END()
