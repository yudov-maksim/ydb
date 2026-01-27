LIBRARY()

GENERATE_ENUM_SERIALIZATION(error.h)

SRCS(
    context.cpp
    error.cpp
    helpers.cpp
    page_size.cpp
    startable.cpp
    thread.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/protos

    library/cpp/lwtrace
    util
    ydb/core/protos/nbs
    ydb/library/services
)

END()
