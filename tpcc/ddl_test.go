package tpcc

import "testing"

func TestAppendPartition(t *testing.T) {
	ddlM := &ddlManager{parts: 3, warehouses: 12, partitionType: PartitionTypeHash, useFK: false}
	got := ddlM.appendPartition("<tbl>", "id")
	expected := `<tbl>
PARTITION BY HASH(id)
PARTITIONS 3`
	if got != expected {
		t.Errorf("got: %s\nexpected: %s", got, expected)
	}

	ddlM = &ddlManager{parts: 3, warehouses: 12, partitionType: PartitionTypeRange, useFK: false}
	got = ddlM.appendPartition("<tbl>", "id")
	expected = `<tbl>
PARTITION BY RANGE (id)
(PARTITION p0 VALUES LESS THAN (5),
 PARTITION p1 VALUES LESS THAN (9),
 PARTITION p2 VALUES LESS THAN (13))`
	if got != expected {
		t.Errorf("got: %s\nexpected: %s", got, expected)
	}

	ddlM = &ddlManager{parts: 3, warehouses: 10, partitionType: PartitionTypeRange, useFK: false}
	got = ddlM.appendPartition("<tbl>", "id")
	expected = `<tbl>
PARTITION BY RANGE (id)
(PARTITION p0 VALUES LESS THAN (5),
 PARTITION p1 VALUES LESS THAN (9),
 PARTITION p2 VALUES LESS THAN (13))`
	if got != expected {
		t.Errorf("got: %s\nexpected: %s", got, expected)
	}

	ddlM = &ddlManager{parts: 3, warehouses: 12, partitionType: PartitionTypeListAsHash, useFK: false}
	got = ddlM.appendPartition("<tbl>", "id")
	expected = `<tbl>
PARTITION BY LIST (id)
(PARTITION p0 VALUES IN (1,4,7,10),
 PARTITION p1 VALUES IN (2,5,8,11),
 PARTITION p2 VALUES IN (3,6,9,12))`
	if got != expected {
		t.Errorf("got: %s\nexpected: %s", got, expected)
	}

	ddlM = &ddlManager{parts: 3, warehouses: 10, partitionType: PartitionTypeListAsHash, useFK: false}
	got = ddlM.appendPartition("<tbl>", "id")
	expected = `<tbl>
PARTITION BY LIST (id)
(PARTITION p0 VALUES IN (1,4,7,10),
 PARTITION p1 VALUES IN (2,5,8),
 PARTITION p2 VALUES IN (3,6,9))`
	if got != expected {
		t.Errorf("got: %s\nexpected: %s", got, expected)
	}

	ddlM = &ddlManager{parts: 3, warehouses: 12, partitionType: PartitionTypeListAsRange, useFK: false}
	got = ddlM.appendPartition("<tbl>", "id")
	expected = `<tbl>
PARTITION BY LIST (id)
(PARTITION p0 VALUES IN (1,2,3,4),
 PARTITION p1 VALUES IN (5,6,7,8),
 PARTITION p2 VALUES IN (9,10,11,12))`
	if got != expected {
		t.Errorf("got: %s\nexpected: %s", got, expected)
	}

	ddlM = &ddlManager{parts: 3, warehouses: 10, partitionType: PartitionTypeListAsRange, useFK: false}
	got = ddlM.appendPartition("<tbl>", "id")
	expected = `<tbl>
PARTITION BY LIST (id)
(PARTITION p0 VALUES IN (1,2,3,4),
 PARTITION p1 VALUES IN (5,6,7),
 PARTITION p2 VALUES IN (8,9,10))`
	if got != expected {
		t.Errorf("got: %s\nexpected: %s", got, expected)
	}

	ddlM = &ddlManager{parts: 3, warehouses: 4, partitionType: PartitionTypeListAsRange, useFK: false}
	got = ddlM.appendPartition("<tbl>", "id")
	expected = `<tbl>
PARTITION BY LIST (id)
(PARTITION p0 VALUES IN (1,2),
 PARTITION p1 VALUES IN (3),
 PARTITION p2 VALUES IN (4))`
	if got != expected {
		t.Errorf("got: %s\nexpected: %s", got, expected)
	}
}
