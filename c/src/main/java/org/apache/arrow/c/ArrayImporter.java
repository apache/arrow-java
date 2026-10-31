/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.c;

import static org.apache.arrow.c.NativeUtil.NULL;
import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.util.Preconditions.checkNotNull;
import static org.apache.arrow.util.Preconditions.checkState;

import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;

/** Importer for {@link ArrowArray}. */
final class ArrayImporter {
  private static final int MAX_IMPORT_RECURSION_LEVEL = 64;

  private final BufferAllocator allocator;
  private final FieldVector vector;
  // The field the vector was created from, or null to use the vector's own field. Unlike the
  // field of an ExtensionTypeVector that does not override getField(), it has the extension type.
  private final Field field;
  private final DictionaryProvider dictionaryProvider;

  private ReferenceCountedArrowArray underlyingAllocation;
  private int recursionLevel;

  ArrayImporter(
      BufferAllocator allocator, FieldVector vector, DictionaryProvider dictionaryProvider) {
    this(allocator, vector, /* field= */ null, dictionaryProvider);
  }

  ArrayImporter(
      BufferAllocator allocator,
      FieldVector vector,
      Field field,
      DictionaryProvider dictionaryProvider) {
    this.allocator = Preconditions.checkNotNull(allocator);
    this.vector = Preconditions.checkNotNull(vector);
    this.field = field;
    this.dictionaryProvider = dictionaryProvider;
  }

  void importArray(ArrowArray src) {
    ArrowArray.Snapshot snapshot = src.snapshot();
    checkState(snapshot.release != NULL, "Cannot import released ArrowArray");

    // Move imported array
    ArrowArray ownedArray = ArrowArray.allocateNew(allocator);
    ownedArray.save(snapshot);
    src.markReleased();

    recursionLevel = 0;

    // This keeps the array alive as long as there are any buffers that need it
    underlyingAllocation = new ReferenceCountedArrowArray(ownedArray);
    try {
      doImport(snapshot);
    } finally {
      underlyingAllocation.release();
    }
  }

  private void importChild(ArrayImporter parent, ArrowArray.Snapshot snapshot) {
    checkState(snapshot.release != NULL, "Cannot import released ArrowArray");
    recursionLevel = parent.recursionLevel + 1;
    checkState(
        recursionLevel <= MAX_IMPORT_RECURSION_LEVEL,
        "Recursion level in ArrowArray struct exceeded");
    // Child buffers will keep the entire parent import alive.
    underlyingAllocation = parent.underlyingAllocation;
    doImport(snapshot);
  }

  private void doImport(ArrowArray.Snapshot snapshot) {
    checkState(snapshot.offset >= 0, "ArrowArray struct has negative offset (%s)", snapshot.offset);
    if (snapshot.offset != 0 && snapshot.length != 0) {
      importSliced(snapshot);
    } else {
      importUnsliced(snapshot, /* skippedRows= */ 0, /* trimChildren= */ false);
    }
  }

  /**
   * Java vectors cannot represent an offset, so the rows up to the end of the slice are imported
   * into a temporary vector and the slice is then transferred into the target. Leading rows that
   * fill whole bytes of a bitmap are skipped by advancing the buffer pointers, so that the
   * temporary vector, and any validity bitmap synthesized for it, has fewer than 8 rows before the
   * slice.
   */
  private void importSliced(ArrowArray.Snapshot snapshot) {
    // Run-end encoded arrays have no buffers, and their children are not indexed by row
    long skippedRows =
        storageType().getTypeID() == ArrowTypeID.RunEndEncoded ? 0 : snapshot.offset & ~7L;
    int offset = checkedCastToInt(snapshot.offset - skippedRows);
    int length = checkedCastToInt(snapshot.length);

    snapshot.offset = 0;
    snapshot.length = Math.addExact(offset, length);
    snapshot.null_count = unknownNullCount(snapshot);

    Field unslicedField = field != null ? field : vector.getField();
    try (FieldVector unslicedVector = unslicedField.createVector(allocator)) {
      // When the target is an extension vector whose getField() reports its storage type, the
      // temporary vector is a storage vector, so transfer between the storage vectors
      FieldVector from = ExtensionTypeVector.getStorageVector(unslicedVector);
      FieldVector to = ExtensionTypeVector.getStorageVector(vector);
      checkTransferable(from, to);
      ArrayImporter unslicedImporter =
          new ArrayImporter(allocator, unslicedVector, unslicedField, dictionaryProvider);
      unslicedImporter.recursionLevel = recursionLevel;
      unslicedImporter.underlyingAllocation = underlyingAllocation;
      unslicedImporter.importUnsliced(snapshot, skippedRows, /* trimChildren= */ true);
      from.makeTransferPair(to).splitAndTransfer(offset, length);
    }
  }

  /**
   * Transfer pairs of nested vectors create or reuse the children of the target based on the
   * classes of the source's children, so every vector of the target must be an instance of the
   * class of its counterpart in the source.
   */
  private static void checkTransferable(FieldVector from, FieldVector to) {
    if (!from.getClass().isInstance(to)) {
      String reason =
          to instanceof ExtensionTypeVector && !(from instanceof ExtensionTypeVector)
              ? "its getField() does not report its extension type"
              : "its field creates a " + from.getClass().getSimpleName();
      throw new UnsupportedOperationException(
          String.format(
              "Cannot import an ArrowArray with a non-zero offset into %s %s, because %s",
              to.getClass().getSimpleName(), to.getName(), reason));
    }
    List<FieldVector> fromChildren = from.getChildrenFromFields();
    List<FieldVector> toChildren = to.getChildrenFromFields();
    for (int i = 0; i < Math.min(fromChildren.size(), toChildren.size()); i++) {
      checkTransferable(fromChildren.get(i), toChildren.get(i));
    }
  }

  /**
   * Imports an array whose offset is 0, skipping the first {@code skippedRows} rows of its buffers,
   * which must be a multiple of 8. With {@code trimChildren}, children whose rows line up with the
   * rows of the array are imported only for the rows of the array, rather than in full.
   */
  private void importUnsliced(
      ArrowArray.Snapshot snapshot, long skippedRows, boolean trimChildren) {
    // First import children (required for reconstituting parent array data)
    long[] children =
        NativeUtil.toJavaArray(snapshot.children, checkedCastToInt(snapshot.n_children));
    if (children != null && children.length > 0) {
      List<FieldVector> childVectors = vector.getChildrenFromFields();
      checkState(
          children.length == childVectors.size(),
          "ArrowArray struct has %s children (expected %s)",
          children.length,
          childVectors.size());
      List<Field> childFields = field != null ? field.getChildren() : null;
      long childRowsPerRow = trimChildren ? childRowsPerRow(storageType()) : 0;
      for (int i = 0; i < children.length; i++) {
        checkState(children[i] != NULL, "ArrowArray struct has NULL child at position %s", i);
        ArrowArray.Snapshot child = ArrowArray.wrap(children[i]).snapshot();
        if (childRowsPerRow != 0) {
          long firstRow = Math.multiplyExact(skippedRows, childRowsPerRow);
          long rowCount = Math.multiplyExact(snapshot.length, childRowsPerRow);
          checkState(
              child.length - firstRow >= rowCount,
              "ArrowArray struct child at position %s has length %s, but its parent needs %s rows",
              i,
              child.length,
              firstRow + rowCount);
          child.offset = Math.addExact(child.offset, firstRow);
          child.length = rowCount;
          child.null_count = unknownNullCount(child);
        }
        Field childField =
            childFields != null && childFields.size() == children.length
                ? childFields.get(i)
                : null;
        ArrayImporter childImporter =
            new ArrayImporter(allocator, childVectors.get(i), childField, dictionaryProvider);
        childImporter.importChild(this, child);
      }
    }

    // Handle import of a dictionary encoded vector
    if (snapshot.dictionary != NULL) {
      DictionaryEncoding encoding = vector.getField().getDictionary();
      checkNotNull(encoding, "Missing encoding on import of ArrowArray with dictionary");

      Dictionary dictionary = dictionaryProvider.lookup(encoding.getId());
      checkNotNull(dictionary, "Dictionary lookup failed on import of ArrowArray with dictionary");

      // reset the dictionary vector to the initial state
      dictionary.getVector().clear();

      ArrayImporter dictionaryImporter =
          new ArrayImporter(allocator, dictionary.getVector(), dictionaryProvider);
      dictionaryImporter.importChild(this, ArrowArray.wrap(snapshot.dictionary).snapshot());
    }

    // Import main data
    ArrowFieldNode fieldNode = new ArrowFieldNode(snapshot.length, snapshot.null_count);
    long[] bufferPointers =
        NativeUtil.toJavaArray(snapshot.buffers, checkedCastToInt(snapshot.n_buffers));

    try (final BufferImportTypeVisitor visitor =
        new BufferImportTypeVisitor(
            allocator, underlyingAllocation, fieldNode, bufferPointers, skippedRows)) {
      final List<ArrowBuf> buffers;
      if (bufferPointers == null || bufferPointers.length == 0) {
        buffers = Collections.emptyList();
      } else {
        buffers = vector.getField().getType().accept(visitor);
      }
      vector.loadFieldBuffers(fieldNode, buffers);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Could not load buffers for field "
              + vector.getField()
              + ". error message: "
              + e.getMessage(),
          e);
    }
  }

  /**
   * Returns the null count to import a different range of the rows of {@code snapshot} with than
   * the range its null count covers. Without a validity bitmap it must be 0 for the buffers to
   * load, otherwise it is unknown.
   */
  private static long unknownNullCount(ArrowArray.Snapshot snapshot) {
    boolean hasBitmap =
        snapshot.n_buffers > 0
            && snapshot.buffers != NULL
            && MemoryUtil.getLong(snapshot.buffers) != NULL;
    return hasBitmap ? -1 : 0;
  }

  /** The type of the vector, or the storage type of an extension type. */
  private ArrowType storageType() {
    ArrowType type = vector.getField().getType();
    return type instanceof ArrowType.ExtensionType
        ? ((ArrowType.ExtensionType) type).storageType()
        : type;
  }

  /**
   * Returns how many rows of each child a row of an array of {@code type} spans, or 0 if its
   * children are not indexed by its rows.
   */
  private static long childRowsPerRow(ArrowType type) {
    switch (type.getTypeID()) {
      case Struct:
        return 1;
      case Union:
        return ((ArrowType.Union) type).getMode() == UnionMode.Sparse ? 1 : 0;
      case FixedSizeList:
        return ((ArrowType.FixedSizeList) type).getListSize();
      default:
        return 0;
    }
  }
}
