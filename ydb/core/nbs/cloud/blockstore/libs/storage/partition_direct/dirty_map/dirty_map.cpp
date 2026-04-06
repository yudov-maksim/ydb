#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TReadRangeHint::TReadRangeHint(
    TLocationMask locationMask,
    ui64 lsn,
    TBlockRange64 requestRelativeRange,
    TBlockRange64 vchunkRange,
    TRangeLock&& lock)
    : LocationMask(locationMask)
    , Lsn(lsn)
    , RequestRelativeRange(requestRelativeRange)
    , VChunkRange(vchunkRange)
    , Lock(std::move(lock))
{}

TReadRangeHint::TReadRangeHint(TReadRangeHint&& other) noexcept = default;
TReadRangeHint& TReadRangeHint::operator=(
    TReadRangeHint&& other) noexcept = default;

TString TReadRangeHint::DebugPrint() const
{
    return TStringBuilder()
           << Lsn << "{" << LocationMask.Print() << VChunkRange.Print()
           << RequestRelativeRange.Print() << "};";
}

TString TReadHint::DebugPrint() const
{
    if (RangeHints.empty()) {
        return (WaitReady.IsReady()) ? "WaitReady:Ready" : "WaitReady:NotReady";
    }

    TStringBuilder result;
    for (const auto& hint: RangeHints) {
        result << hint.DebugPrint();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TString TPBufferSegment::DebugPrint() const
{
    return TStringBuilder() << Lsn << Range.Print();
}

TString TFlushHint::DebugPrint() const
{
    TStringBuilder builder;
    bool first = true;
    for (const auto& segment: Segments) {
        if (!first) {
            builder << ",";
        }
        builder << segment.DebugPrint();
        first = false;
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

void TFlushHints::AddHint(
    ELocation source,
    ELocation destination,
    ui64 lsn,
    TBlockRange64 range)
{
    Hints[TRoute{.Source = source, .Destination = destination}]
        .Segments.emplace_back(lsn, range);
}

bool TFlushHints::Empty() const
{
    return Hints.empty();
}

const TFlushHints::THints& TFlushHints::GetAllHints() const
{
    return Hints;
}

TFlushHints::THints TFlushHints::TakeAllHints()
{
    return std::move(Hints);
}

TString TFlushHints::DebugPrint() const
{
    TStringBuilder builder;
    for (const auto& [l, hint]: Hints) {
        builder << ToString(l.Source) << "->" << ToString(l.Destination) << ":"
                << hint.DebugPrint() << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TString TEraseHint::DebugPrint() const
{
    TStringBuilder builder;
    bool first = true;
    for (const auto& segment: Segments) {
        if (!first) {
            builder << ",";
        }
        builder << segment.DebugPrint();
        first = false;
    }
    return builder;
}

void TEraseHints::AddHint(ELocation location, ui64 lsn, TBlockRange64 range)
{
    Hints[location].Segments.emplace_back(lsn, range);
}

bool TEraseHints::Empty() const
{
    return Hints.empty();
}

const TEraseHints::THints& TEraseHints::GetAllHints() const
{
    return Hints;
}

TEraseHints::THints TEraseHints::TakeAllHints()
{
    return std::move(Hints);
}

TString TEraseHints::DebugPrint() const
{
    TStringBuilder builder;
    for (const auto& [l, hint]: Hints) {
        builder << ToString(l) << ":" << hint.DebugPrint() << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueues,
    ui64 lsn,
    ELocation location)
    : State(EState::PBufferIncompleteWrite)
    , ReadyQueue(readyQueues)
    , Lsn(lsn)
{
    WriteRequested.Set(location);
    WriteConfirmed.Set(location);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Clone);
}

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueue,
    ui64 lsn,
    TLocationMask writeRequested,
    TLocationMask writeConfirmed)
    : State(EState::PBufferWritten)
    , ReadyQueue(readyQueue)
    , Lsn(lsn)
    , WriteRequested(writeRequested)
    , WriteConfirmed(writeConfirmed)
{
    Y_ABORT_UNLESS(WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount);

    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
}

TInflightInfo::TInflightInfo(TInflightInfo&& other) noexcept
    : State(other.State)
    , ReadyQueue(other.ReadyQueue)
    , Lsn(other.Lsn)
    , WriteRequested(other.WriteRequested)
    , WriteConfirmed(other.WriteConfirmed)
    , FlushRequested(other.FlushRequested)
    , FlushConfirmed(other.FlushConfirmed)
{
    other.ReadyQueue = nullptr;
}

TInflightInfo::~TInflightInfo()
{
    Y_ABORT_UNLESS(PBuffersLockCount == 0);
}

void TInflightInfo::RestorePBuffer(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(
        State == EState::PBufferIncompleteWrite ||
        State == EState::PBufferWritten);
    Y_ABORT_UNLESS(!WriteRequested.Get(location));
    Y_ABORT_UNLESS(!WriteConfirmed.Get(location));

    WriteRequested.Set(location);
    WriteConfirmed.Set(location);

    if (WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount) {
        if (QuorumReadyPromise.Initialized()) {
            QuorumReadyPromise.TrySetValue();
        }

        State = EState::PBufferWritten;
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
    }
}

TInflightInfo::EState TInflightInfo::GetState() const
{
    return State;
}

NThreading::TFuture<void> TInflightInfo::GetQuorumReadyFuture()
{
    if (!QuorumReadyPromise.Initialized()) {
        QuorumReadyPromise = NThreading::NewPromise<void>();
    }
    return QuorumReadyPromise.GetFuture();
}

TLocationMask TInflightInfo::ReadMask() const
{
    switch (State) {
        case EState::PBufferIncompleteWrite:
            // Reading will be possible only after receiving a quorum.
            return TLocationMask::MakeEmpty();

        case EState::PBufferWritten:
        case EState::PBufferFlushing:
            // The data is written to PBuffer, but not transferred to DDisk.
            // Will read from confirmed PBuffer.
            return WriteConfirmed;

        case EState::PBufferFlushed:
        case EState::PBufferErasing:
        case EState::PBufferErased:
            // The data has already been transferred to DDisk.
            // Will read from DDisks.
            // Filter out non-desired or fresh later.
            return TLocationMask::MakeAllDDisks();
    }
}

ELocation TInflightInfo::RequestFlush(ELocation destination)
{
    Y_ABORT_UNLESS(IsDDisk(destination));
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing);

    FlushDesired.Set(destination);

    if (FlushRequested.Get(destination)) {
        return ELocation::Unknown;
    }

    const ELocation bestSource = TranslateDDiskToPBuffer(destination);
    if (WriteConfirmed.Get(bestSource)) {
        State = EState::PBufferFlushing;
        FlushRequested.Set(destination);
        return bestSource;
    }

    for (ELocation source: PBufferLocations) {
        if (WriteConfirmed.Get(source)) {
            State = EState::PBufferFlushing;
            FlushRequested.Set(destination);
            return source;
        }
    }

    Y_ABORT_UNLESS(false);
}

void TInflightInfo::ConfirmFlush(TRoute route)
{
    Y_ABORT_UNLESS(IsDDisk(route.Destination));
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(route.Destination));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(route.Destination));

    FlushConfirmed.Set(route.Destination);

    if (FlushDesired == FlushConfirmed) {
        State = EState::PBufferFlushed;
    }

    if (State == EState::PBufferFlushed && PBuffersLockCount == 0) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }
}

void TInflightInfo::FlushFailed(TRoute route)
{
    Y_ABORT_UNLESS(IsDDisk(route.Destination));
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(route.Destination));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(route.Destination));

    FlushRequested.Reset(route.Destination);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
}

bool TInflightInfo::RequestErase(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(
        State == EState::PBufferFlushed || State == EState::PBufferErasing);
    Y_ABORT_UNLESS(FlushConfirmed.Count() >= QuorumDirectBlockGroupHostCount);

    if (WriteRequested.Get(location) && !EraseRequested.Get(location)) {
        State = EState::PBufferErasing;
        EraseRequested.Set(location);
        return true;
    }
    return false;
}

bool TInflightInfo::ConfirmErase(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(EraseRequested.Get(location));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(location));

    EraseConfirmed.Set(location);
    if (EraseConfirmed == EraseRequested) {
        State = EState::PBufferErased;
    }

    return State == EState::PBufferErased;
}

void TInflightInfo::EraseFailed(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(!EraseConfirmed.Get(location));

    EraseRequested.Reset(location);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
}

void TInflightInfo::LockPBuffer()
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing ||
        State == EState::PBufferFlushed);

    ++PBuffersLockCount;

    if (PBuffersLockCount == 1) {
        ReadyQueue->UnRegister(Lsn);
    }
}

void TInflightInfo::UnlockPBuffer()
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing ||
        State == EState::PBufferFlushed);
    Y_ABORT_UNLESS(PBuffersLockCount > 0);

    --PBuffersLockCount;

    if (State == EState::PBufferFlushed && PBuffersLockCount == 0) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TBlocksDirtyMap::UpdateConfig(
    TLocationMask desired,
    TLocationMask disabled)
{
    Y_ABORT_UNLESS(disabled.LogicalAnd(desired).Empty());

    DesiredDDisks = desired.LogicalAnd(TLocationMask::MakeAllDDisks());
    DesiredPBuffers = desired.LogicalAnd(TLocationMask::MakeAllPBuffers());
    DisabledLocations = disabled;
}

void TBlocksDirtyMap::RestorePBuffer(
    ui64 lsn,
    TBlockRange64 range,
    ELocation location)
{
    if (auto item = Inflight.GetValue(lsn)) {
        Y_ABORT_UNLESS(item->Range == range);

        auto& inflight = item->Value;
        inflight.RestorePBuffer(location);
    } else {
        Inflight.AddRange(lsn, range, TInflightInfo(this, lsn, location));
    }
}

TFlushHints TBlocksDirtyMap::MakeFlushHint(size_t batchSize)
{
    TFlushHints result;

    if (ReadyToFlush.size() < batchSize) {
        return result;
    }

    THashSet<ui64> readyToFlush;
    readyToFlush.swap(ReadyToFlush);

    for (ui64 lsn: readyToFlush) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& val = item->Value;

        if (InflightDDiskReads.HasOverlaps(item->Range)) {
            // Can't flush to DDisk during reading from overlapped range.
            ReadyToFlush.insert(lsn);
            continue;
        }

        for (ELocation destination: DesiredDDisks) {
            const ELocation source = val.RequestFlush(destination);
            if (source != ELocation::Unknown) {
                result.AddHint(source, destination, item->Key, item->Range);
            }
        }
    }

    return result;
}

TEraseHints TBlocksDirtyMap::MakeEraseHint(size_t batchSize)
{
    TEraseHints result;

    if (ReadyToErase.size() < batchSize) {
        return result;
    }

    THashSet<ui64> readyToErase;
    readyToErase.swap(ReadyToErase);

    for (ui64 lsn: readyToErase) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);

        auto& val = item->Value;

        for (auto l: PBufferLocations) {
            if (!DisabledLocations.Get(l) && val.RequestErase(l)) {
                result.AddHint(l, item->Key, item->Range);
            }
        }
    }

    return result;
}

void TBlocksDirtyMap::WriteFinished(
    ui64 lsn,
    TBlockRange64 range,
    TLocationMask requested,
    TLocationMask confirmed)
{
    const bool inserted = Inflight.AddRange(
        lsn,
        range,
        TInflightInfo(this, lsn, requested, confirmed));
    Y_ABORT_UNLESS(inserted);
}

void TBlocksDirtyMap::FlushFinished(
    TRoute route,
    const TVector<ui64>& flushOk,
    const TVector<ui64>& flushFailed)
{
    for (ui64 lsn: flushOk) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.ConfirmFlush(route);
    }

    for (ui64 lsn: flushFailed) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.FlushFailed(route);
    }
}

void TBlocksDirtyMap::EraseFinished(
    ELocation location,
    const TVector<ui64>& eraseOk,
    const TVector<ui64>& eraseFailed)
{
    for (ui64 lsn: eraseOk) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        if (inflight.ConfirmErase(location)) {
            const bool removed = Inflight.RemoveRange(item->Key);
            Y_ABORT_UNLESS(removed);
        }
    }

    for (ui64 lsn: eraseFailed) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.EraseFailed(location);
    }
}

size_t TBlocksDirtyMap::GetInflightCount() const
{
    return Inflight.Size();
}

void TBlocksDirtyMap::LockPBuffer(ui64 lsn)
{
    auto item = Inflight.GetValue(lsn);
    Y_ABORT_UNLESS(item.has_value());
    item->Value.LockPBuffer();
}

void TBlocksDirtyMap::UnlockPBuffer(ui64 lsn)
{
    auto item = Inflight.GetValue(lsn);
    Y_ABORT_UNLESS(item.has_value());
    item->Value.UnlockPBuffer();
}

ILockableRanges::TLockRangeHandle TBlocksDirtyMap::LockDDiskRange(
    TBlockRange64 range)
{
    const TLockRangeHandle handle = ++InflightDDiskReadsGenerator;
    InflightDDiskReads.AddRange(handle, range);
    return handle;
}

void TBlocksDirtyMap::UnLockDDiskRange(TLockRangeHandle handle)
{
    InflightDDiskReads.RemoveRange(handle);
}

void TBlocksDirtyMap::Register(ui64 lsn, EQueueType queueType)
{
    switch (queueType) {
        case IReadyQueue::EQueueType::Clone: {
            ReadyToClone.insert(lsn);

            ReadyToFlush.erase(lsn);
            ReadyToErase.erase(lsn);
            break;
        }
        case IReadyQueue::EQueueType::Flush: {
            ReadyToFlush.insert(lsn);

            ReadyToClone.erase(lsn);
            ReadyToErase.erase(lsn);
            break;
        }
        case IReadyQueue::EQueueType::Erase: {
            ReadyToErase.insert(lsn);

            ReadyToClone.erase(lsn);
            ReadyToFlush.erase(lsn);
            break;
        }
    }
}

void TBlocksDirtyMap::UnRegister(ui64 lsn)
{
    ReadyToErase.erase(lsn);
    ReadyToClone.erase(lsn);
    ReadyToFlush.erase(lsn);
}

TReadHint TBlocksDirtyMap::MakeReadHint(TBlockRange64 range)
{
    TReadHint result;

    auto makeDefaultHint = [this](TBlockRange64 range, ui64 offsetBlocks)
    {
        auto locationMask = DesiredDDisks.Exclude(DisabledLocations);
        Y_ABORT_UNLESS(!locationMask.Empty());

        return TReadRangeHint(
            locationMask,
            0,   // LSN
            TBlockRange64::WithLength(
                offsetBlocks,
                range.Size()),   // RequestRelativeRange
            range,               // VChunkRange
            TRangeLock(this, range));
    };

    auto makeHint = [this](
                        TLocationMask locationMask,
                        ui64 lsn,
                        TBlockRange64 range,
                        ui64 offsetBlocks)
    {
        Y_ABORT_UNLESS(!locationMask.Empty());

        if (locationMask.HasDDisk()) {
            locationMask = locationMask.LogicalAnd(DesiredDDisks);
        }
        locationMask = locationMask.Exclude(DisabledLocations);
        Y_ABORT_UNLESS(!locationMask.Empty());

        return TReadRangeHint(
            locationMask,
            lsn,
            TBlockRange64::WithLength(
                offsetBlocks,
                range.Size()),   // RequestRelativeRange
            range,               // VChunkRange
            locationMask.OnlyDDisk() ? TRangeLock(this, range)
                                     : TRangeLock(this, lsn));
    };

    if (!Inflight.HasOverlaps(range)) {
        result.RangeHints.push_back(makeDefaultHint(range, 0));
        return result;
    }

    // Собрать все перекрывающиеся inflight записи
    // Используем легковесную структуру с указателем на Value вместо ссылки
    struct TOverlappingItem
    {
        ui64 Key;
        TBlockRange64 Range;
        TInflightInfo* Value;
    };

    TVector<TOverlappingItem> overlappingItems;
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& item)
        {
            overlappingItems.push_back({item.Key, item.Range, &item.Value});
            return TInflightMap::EEnumerateContinuation::Continue;
        });

    // Сортировать по началу диапазона, затем по LSN (убывание для более свежих)
    Sort(
        overlappingItems.begin(),
        overlappingItems.end(),
        [](const auto& a, const auto& b)
        {
            if (a.Range.Start != b.Range.Start) {
                return a.Range.Start < b.Range.Start;
            }
            // При одинаковом начале диапазона, больший LSN первым
            return a.Key > b.Key;
        });

    // Отфильтровать дубликаты - для одинаковых диапазонов оставить только
    // первый (с максимальным LSN)
    TVector<TOverlappingItem> filteredItems;
    for (size_t i = 0; i < overlappingItems.size(); ++i) {
        // Пропустить, если следующий элемент имеет тот же диапазон
        if (i + 1 < overlappingItems.size() &&
            overlappingItems[i].Range == overlappingItems[i + 1].Range)
        {
            // Текущий элемент имеет больший LSN (из-за сортировки), оставляем
            // его
            filteredItems.push_back(overlappingItems[i]);
            // Пропускаем все последующие с тем же диапазоном
            while (i + 1 < overlappingItems.size() &&
                   overlappingItems[i].Range == overlappingItems[i + 1].Range)
            {
                ++i;
            }
        } else {
            filteredItems.push_back(overlappingItems[i]);
        }
    }

    // Разбить на сегменты
    ui64 currentPos = range.Start;
    ui64 offsetBlocks = 0;

    for (const auto& item: filteredItems) {
        if (item.Range.End < currentPos) {
            continue;
        }

        // Добавить gap до текущего item (если есть)
        if (currentPos < item.Range.Start) {
            auto gapRange = TBlockRange64::MakeClosedInterval(
                currentPos,
                Min(item.Range.Start - 1, range.End));

            result.RangeHints.push_back(
                makeDefaultHint(gapRange, offsetBlocks));

            offsetBlocks += gapRange.Size();
            currentPos = item.Range.Start;
        }

        // не нужно?
        if (currentPos > range.End) {
            break;
        }

        // Добавить пересечение с item
        auto intersection = range.Intersect(item.Range);
        // Y_ABORT_UNLESS(offsetBlocks == intersection.Start);

        if (item.Value->ReadMask().Empty()) {
            // Нужно ждать quorum
            result.WaitReady = item.Value->GetQuorumReadyFuture();
            result.RangeHints.clear();
            return result;
        }

        result.RangeHints.push_back(makeHint(
            item.Value->ReadMask(),
            item.Value->ReadMask().OnlyDDisk() ? 0
                                               : item.Key,   // LSN=0 для DDisk
            intersection,
            offsetBlocks));   // intersection.Start

        offsetBlocks += intersection.Size();
        currentPos = intersection.End + 1;
    }

    // Добавить оставшийся gap (если есть)
    if (currentPos <= range.End) {
        auto gapRange =
            TBlockRange64::MakeClosedInterval(currentPos, range.End);

        result.RangeHints.push_back(makeDefaultHint(gapRange, offsetBlocks));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
