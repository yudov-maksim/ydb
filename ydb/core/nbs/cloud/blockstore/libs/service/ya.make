LIBRARY()

GENERATE_ENUM_SERIALIZATION(request.h)

SRCS(
    context.cpp
)

PEERDIR(
    library/cpp/lwtrace
    util
)

END()
