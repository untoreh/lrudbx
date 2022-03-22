An LRU cache sitting on top of mdbx.

## NOTES
- It lacks generics, works only with strings.
- There are no checks for reserved keys, should be added.
- It is slow because of the serialization, it could be avoided by using another collection for the linked list indexing, but the _hooks_ lack the context to use them for this purpose.
