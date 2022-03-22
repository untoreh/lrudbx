import nimdbx
import nimdbx / [CRUD, Cursor, Collatable]
import tables
import macros
import sugar
import hashes
import sets
import frosty/streams

{.experimental: "notnil".}

type
    trNode = tuple[prev: int, value: string, next: int]
    CollectionNotNil = Collection not nil
    CollectionCache = Table[string, CollectionNotNil]
    LruDbx* = ref object
        db: nimdbx.Database.Database
        coll: CollectionCache

const
    headKey = int64.low
    stubHeadKey = int64.low+1
    tailKey = int64.low+2
    stubTailKey = int64.low+3
    sizeKey = int64.low+4
    reservedKeys = [headKey, stubHeadKey, tailKey, stubTailKey, sizeKey].toHashSet
    MAX_COLLECTIONS = 30
    MAX_DB_SIZE = 4096 * 1024 * 1024
    MAX_PAIR_COLLECTION_SIZE = MAX_DB_SIZE.div MAX_COLLECTIONS

proc checkHeadTail(c: Collection not nil) =
    c.inTransaction do (ct: CollectionTransaction):
        if ct.get(sizeKey).asByteSeq.len == 0:
            # head
            ct[headKey] = stubHeadKey
            ct[stubHeadKey] = freeze((prev: 0, value: "", next: stubTailKey))
            # tail
            ct[tailKey] = stubTailKey
            ct[stubTailKey] = freeze((prev: stubHeadKey, value: "", next: 0))
            # size
            ct[sizeKey] = (8 * 4).int64
        ct.commit()

proc getCollection(k: string, cache: LruDbx): Collection not nil =
    try:
        return cache.coll[k]
    except:
        var c: Collection = cache.db.openCollectionOrNil(k, keytype = IntegerKeys)
        var cnn: Collection not nil
        if c.isnil:
            c = cache.db.createCollection(k, keytype = IntegerKeys)
            if c.isnil:
                raise newException(ValueError, "Cannot create collection")
            else:
                cnn = c
        else:
            cnn = c
        checkHeadTail(cnn)
        cache.coll[k] = cnn
        return cnn

proc `$`(cache: LruDbx): string =
    cache.db.path & " with " & $len(cache.coll) & " collections."

proc initLruDbx(path: string, colls: seq[string] = @[]): LruDbx =
    new(result)
    result.db = openDatabase(path, maxFileSize = MAX_DB_SIZE,
            maxcollections = MAX_COLLECTIONS)
    for c in colls:
        discard getCollection(c, result)

macro doTx(coll: string, what: untyped): untyped =
    result = quote do:
        getCollection(`coll`).inTransaction do (ct {.inject.}: CollectionTransaction):
            `what`
            ct.commit()

macro doSnap(coll: string, what: untyped): untyped =
    result = quote do:
        pairCollection(`coll`).inSnapshot do (cs {.inject.}: CollectionSnapshot):
            `what`

proc `[]`*(cache: LruDbx, k: string): Collection not nil =
    getCollection(k, cache)

proc nodeSize(node: trNode): int {.inline.} =
    len(node.value) * sizeof(node.value) + 8 + 8 # prev and next keys are int64

proc adjustSize(ct: CollectionTransaction, thisNodeKey: int64, v: string) =
    let
        collSize = ct[sizeKey].int64
        headNodeKey = ct[headKey].int64
    var
        newSize = collSize + len(v) * sizeof(v) + 8 + 8
        tailNode: trNode
        thisNode: trNode
        initialTailKey = ct[tailKey].int64
        curNodeKey = initialTailKey
        tailNodeKey = initialTailKey

    # check if key already exists and subtract its size
    let thisNodeDataOut = ct[thisNodeKey]
    if thisNodeDataOut.asData.len > 0:
        thisNode = thaw[trNode](thisNodeDataOut)
        newSize -= thisNode.nodeSize

    # keep deleting nodes from the tail until the
    # new size is within bound
    # echo "looping size"
    var i = 0
    while newSize > MAX_PAIR_COLLECTION_SIZE:
        # echo "trying to decode node with key: ", curNodeKey
        tailNode = thaw[trNode](ct[curNodeKey])
        # echo fmt"success! next node going to be {tailNode.prev}"
        tailNodeKey = curNodeKey
        ct.del(curNodeKey)
        i += 1
        newSize -= tailNode.nodeSize
        if tailNode.prev == headNodeKey:
            # echo "Stopping because we reached head"
            break
        elif tailNode.prev == thisNodeKey:
            # echo "The tail previous node is this node"
            curNodeKey = thisNode.prev
        else:
            # echo "setting current node to previous node"
            curNodeKey = tailNode.prev

    # echo fmt"removed {i} nodes ", "updating? ", tailNodeKey != initialTailKey, fmt" newsize? {newSize}, {collSize}"
    # echo "tailNodekey: ", tailNodeKey, " tailnodePrev: ", tailNode.prev, " initialKey: ", initialTailKey
    if tailNodeKey != curNodeKey:
        # update the new tail
        ct[tailKey] = tailNodeKey
        ct[tailNodeKey] = freeze((prev: tailNode.prev,
                                  value: tailNode.value,
                                  next: 0))
    # update size
    if newSize != collSize:
        ct[sizeKey] = newSize

proc `[]=`*(c: Collection not nil, k: Hash, v: string) =
    c.inTransaction do (ct: CollectionTransaction):
        let
            thisKey = k.int64
            headNodeKey = ct[headKey].int64
            headNode = thaw[trNode](ct[headNodeKey])

        adjustSize(ct, thisKey, v)

        if thisKey == headNodeKey:
            ct[thisKey] = freeze((prev: 0,
                                value: v,
                                next: headNode.next))
        else:
            ct[thisKey] = freeze((prev: 0,
                                  value: v,
                                  next: headNodeKey))
            ct[headNodeKey] = freeze((prev: thisKey,
                                    value: headNode.value,
                                    next: headNode.next))
            ct[headKey] = thisKey
        ct.commit()

template `[]=`*(c: CollectionNotNil, k: string, v: untyped) =
    c[hash(k)] = v

proc `[]`*(c: Collection not nil, k: Hash): string =
    var o: string
    c.inTransaction do (ct: CollectionTransaction):
        var thisNode: trNode
        let
            thisKey = k.int64
            headNodeKey = ct[headKey]
            thisDataOut = ct[thisKey]
        if thisDataOut.asData.len != 0:
            thisNode = thaw[trNode](thisDataOut)
            if thisKey == headNodekey:
                o = thisNode.value
            else:
                let headNode = thaw[trNode](ct[headNodeKey])
                ct[thisKey] = freeze((prev: 0,
                                             value: thisNode.value,
                                             next: headNodeKey))
                ct[headNodeKey] = freeze((prev: thisKey,
                                          value: headNode.value,
                                          next: headNode.next))
                ct[headKey] = thisKey
                o = thisNode.value
        ct.commit()
    return o

template `[]`*(c: CollectionNotNil, k: string) =
    c[hash(k)]

template get*(c, k: string, def) =
    c.get(hash(k))

proc get*(c: CollectionNotNil, k: Hash, def: string): string =
    result = c[k]
    if result == "":
        result = def

template del*(c, k: string) =
    c.del(hash(k))

proc del*(c: CollectionNotNil, k: Hash) =
    c.inTransaction do (ct: CollectionTransaction):
        let
            thisKey = k.int64
            headNodeKey = ct[headKey].asInt64
            tailNodeKey = ct[tailKey].asInt64
            thisDataOut = ct[thisKey.asdata]
        if thisDataOut.asData.len != 0:
            let thisNode = thaw[trNode](thisDataOut)
            ct.del(thisKey.asdata)
            if thisKey == headNodeKey:
                let newHeadNodeKey = thisNode.next.int64
                let newHeadNode = thaw[trNode](ct[newHeadNodeKey])
                ct[newHeadNodeKey] = freeze((prev: nil,
                                    value: newHeadNode.value,
                                    next: newHeadNode.next))
                ct[headKey] = newHeadNodeKey
            elif thisKey == tailNodeKey:
                let
                    newTailNodeKey = thisNode.prev.asdata
                    newTailNode = thaw[trNode](ct[newTailNodeKey])
                ct[newTailNodeKey] = freeze((prev: newTailNode.prev,
                                    value: newTailNode.value,
                                    next: nil))
                ct[tailKey] = newTailNodeKey
            else:
                let
                    prevNodeKey = thisNode.prev.asdata
                    nextNodeKey = thisNode.next.asdata
                    prevNode = thaw[trNode](ct[prevNodeKey])
                    nextNode = thaw[trNode](ct[nextNodeKey])
                ct[prevNodeKey] = freeze((prev: prevNode.prev,
                                          value: prevNode.value,
                                          next: nextNodeKey))
                ct[nextNodeKey] = freeze((prev: prevNodeKey,
                                          value: nextNode.value,
                                          next: nextNode.next))
            ct.commit()

template cursIter(c: Collection, what: untyped): untyped =
    let cs = c.beginSnapshot
    defer: cs.finish()
    var curs {.inject.} = makeCursor(cs)
    curs.first
    # FIXME: the first key is skipped in the iteration...
    if curs.toBool():
        if not (curs.key.int64 in reservedKeys):
            yield what
        while curs.next():
            if curs.key in reservedKeys:
                continue
            yield what

iterator keys*(c: Collection not nil): int64 =
    cursIter(c, curs.key.int64)

iterator values*(c: Collection not nil): trNode =
    cursIter(c, thaw[trNode](curs.value))


iterator items*(c: Collection not nil): (int64, trNode) =
    cursIter(c, (curs.key.int64, thaw[trNode](curs.value)))

proc clear*(c: Collection not nil) =
    let ks = collect(for k in keys(c): k)
    c.inTransaction do (ct: CollectionTransaction):
        for k in ks:
            ct.del(k.int64)
        ct.commit()

proc sizeof*(c: Collection not nil): int =
    var size: int
    c.inSnapshot do (cs: CollectionSnapshot):
        size = cs[sizeKey].int
    return size

proc del*(cache: LruDbx, coll: string) =
    ## NOTE: deleting collections requires DB reopen
    cache[coll].inTransaction do (ct: CollectionTransaction):
        ct.deleteCollection()
        cache.coll.del(coll)
        ct.commit()
    cache.db.close
    let path = cache.db.path
    cache.db = openDatabase(path, maxFileSize = MAX_DB_SIZE,
            maxcollections = MAX_COLLECTIONS)
    for c in cache.coll.keys:
        discard getCollection(c, cache)

proc hasKey*(c: CollectionNotNil, k: int64): bool =
    var res: bool
    c.inSnapshot do (cs: CollectionSnapshot):
        res = cs[k].asData.len != 0
    res

proc hasKey*(c: CollectionNotNil, k: string): bool =
    c.hasKey hash(k).int64
