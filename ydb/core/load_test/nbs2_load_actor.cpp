#include "service_actor.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <ydb/library/workload/abstract/workload_factory.h>
#include <ydb/library/workload/stock/stock.h>
#include <ydb/library/workload/kv/kv.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/histogram/hdr/histogram.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/queue.h>
#include <util/random/fast.h>
#include <util/random/shuffle.h>


namespace NKikimr {

enum {
    EvNBS2WorkerResponse
};

class TNBS2LoadActor : public TActorBootstrapped<TNBS2LoadActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::NBS2_TEST_WORKLOAD;
    }

    TNBS2LoadActor(const NKikimr::TEvLoadTestRequest::TNBS2Load& cmd, const TActorId& parent,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag)
        : Parent(parent)
        , Tag(tag)
        , DurationSeconds(cmd.GetDurationSeconds())
        , TestParam(cmd.GetTestParam())
        , Name(cmd.GetName())
    {
        Y_UNUSED(index);
        Y_UNUSED(counters);
        VERIFY_PARAM(DurationSeconds);
        google::protobuf::TextFormat::PrintToString(cmd, &ConfigString);
    }

    ~TNBS2LoadActor() {
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " TNBS2LoadActor Bootstrap called");

        Become(&TNBS2LoadActor::StateStart);
        // TODO extend with vdisk_write code?

        LOG_INFO_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " Schedule PoisonPill");

        ctx.Schedule(TDuration::Seconds(DurationSeconds + 1), new TEvents::TEvPoisonPill);

    }

    STRICT_STFUNC(StateStart,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(NMon::TEvHttpInfo, HandleHTML)
    )

private:

    // death

    void HandlePoisonPill(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        // TODO copypast from vdisk_write
    }

private:


    TString RenderHTML() {
        TStringStream str;
        HTML(str) {
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {
                            str << "DurationSeconds";
                        }
                        TABLEH() {
                            str << "TestParam";
                        }
                        TABLEH() {
                            str << "Name";
                        }
                    }
                }
                TABLEBODY() {
                    TABLER() {
                        TABLED() {
                            str << DurationSeconds;
                        };
                        TABLED() {
                            str << TestParam;
                        };
                        TABLED() {
                            str << Name;
                        };
                    }
                }
            }
            COLLAPSED_BUTTON_CONTENT(Sprintf("configProtobuf%" PRIu64, Tag), "Config") {
                str << "<pre>" << ConfigString << "</pre>";
            }
        }
        return str.Str();
    }

    void HandleHTML(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderHTML(), ev->Get()->SubRequestId));
    }

    // common

    const TActorId Parent;
    ui64 Tag;
    ui32 DurationSeconds;
    TString TestParam;
    TString Name;

    // ---
    TString ConfigString;
};

IActor * CreateNBS2LoadActor(const NKikimr::TEvLoadTestRequest::TNBS2Load& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TNBS2LoadActor(cmd, parent, counters, index, tag);
}

} // NKikimr
