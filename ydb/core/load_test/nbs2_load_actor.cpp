#include "service_actor.h"

#include "nbs2_lib/app_context.h"
#include "nbs2_lib/suite_runner.h"

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
        , RangeTest(cmd.GetRangeTest())
    {
        Y_UNUSED(index);
        Y_UNUSED(counters);
        VERIFY_PARAM(DurationSeconds);
        VERIFY_PARAM(RangeTest);
        google::protobuf::TextFormat::PrintToString(cmd, &ConfigString);
    }

    ~TNBS2LoadActor() {
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " TNBS2LoadActor Bootstrap called");

        Become(&TNBS2LoadActor::StateStart);
        LOG_INFO_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " Schedule PoisonPill");

        ctx.Schedule(TDuration::Seconds(DurationSeconds + 1), new TEvents::TEvPoisonPill);
        DoLoadTest(ctx);
    }

     NCloud::NBlockStore::NProto::ETestStatus RunTest(
        const TActorContext& ctx,
        NCloud::NBlockStore::NLoadTest::TAppContext& appContext,
        NCloud::NBlockStore::NLoadTest::TTestContext& testContext
    ) {
        using namespace NCloud::NBlockStore::NLoadTest;
        LOG_DEBUG_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " RunTest called");


        TSuiteRunner suiteRunner(
            appContext,
            Name,
            testContext
        );

        //for (const auto& range: test.GetRanges()) {
        suiteRunner.StartSubtest(RangeTest);

        suiteRunner.Wait(DurationSeconds); // todo проверить ожидаемые единицы измерения

        const auto& suiteResults = suiteRunner.GetResults();
        // todo заполнить result
        // NProto::TTestResults proto; ...
        return suiteResults.Status;
     }

    void DoLoadTest(const TActorContext& ctx) {
        using namespace NCloud::NBlockStore::NLoadTest;
        LOG_DEBUG_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " DoLoadTest called");

        TAppContext appContext;
        TTestContext testContext; // todo print Result

        try {
            // SetupTest(test, dependencies, SuccessOnError(test));
            // кажется, проверка ShouldStop тут не нужна
            if (!appContext.ShouldStop.load(std::memory_order_acquire)) {
                auto testResult = RunTest(ctx, appContext, testContext);
                if (testResult == NCloud::NBlockStore::NProto::TEST_STATUS_FAILURE) {
                    appContext.FailedTests.fetch_add(1);
                    return; // return 1;
                }
            }

            // TeardownTest(test, SuccessOnError(test));
        } catch (...) {
            LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Exception during test execution: "
                << CurrentExceptionMessage());
            appContext.FailedTests.fetch_add(1);
            return;
            //return EC_LOAD_TEST_FAILED;
        }
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
    NCloud::NBlockStore::NProto::TRangeTest RangeTest;

    // ---
    TString ConfigString;
};

IActor * CreateNBS2LoadActor(const NKikimr::TEvLoadTestRequest::TNBS2Load& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TNBS2LoadActor(cmd, parent, counters, index, tag);
}

} // NKikimr
