# test_buffer.py

import sys
import unittest
from Storage_Manager.lib.Block import Block, BLOCK_SIZE, DATA_SIZE
from Buffer import Buffer, DoublyLinkedListNode, DoublyLinkedList
from typing import ByteString

class ColoredTextTestResult(unittest.TextTestResult):
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"

    def addSuccess(self, test):
        super().addSuccess(test)
        print(f" {self.GREEN}SUCCESS: {test}{self.RESET}")
        sys.stdout.flush()

    def addFailure(self, test, err):
        super().addFailure(test, err)
        print(f" {self.RED}FAIL: {test}{self.RESET}")
        sys.stdout.flush()

    def addError(self, test, err):
        super().addError(test, err)
        print(f" {self.RED}ERROR: {test}{self.RESET}")
        sys.stdout.flush()


class ColoredTextTestRunner(unittest.TextTestRunner):
    resultclass = ColoredTextTestResult
class TestBuffer(unittest.TestCase):
    def setUp(self):
        """
        Initialize the Buffer instance and create several Block instances for testing.
        """
        self.buffer_capacity = 3
        self.buffer = Buffer(capacity=self.buffer_capacity)
        
        # Create Block instances with distinct data
        self.block1 = Block()
        self.block1.header["page_id"] = 1
        self.block1.add_record(b"Record1_Data")
        
        self.block2 = Block()
        self.block2.header["page_id"] = 2
        self.block2.add_record(b"Record2_Data")
        
        self.block3 = Block()
        self.block3.header["page_id"] = 3
        self.block3.add_record(b"Record3_Data")
        
        self.block4 = Block()
        self.block4.header["page_id"] = 4
        self.block4.add_record(b"Record4_Data")
        
        self.block5 = Block()
        self.block5.header["page_id"] = 5
        self.block5.add_record(b"Record5_Data")
    
    def test_set_and_get(self):
        """
        Test basic set and get operations.
        """
        # Add three blocks
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableA", 2, self.block2)
        self.buffer.set("TableB", 1, self.block3)
        
        # Retrieve blocks and verify
        retrieved1 = self.buffer.get("TableA", 1)
        self.assertEqual(retrieved1.header["page_id"], 1)
        retrieved2 = self.buffer.get("TableA", 2)
        self.assertEqual(retrieved2.header["page_id"], 2)
        retrieved3 = self.buffer.get("TableB", 1)
        self.assertEqual(retrieved3.header["page_id"], 3)
    
    def test_update_existing_block(self):
        """
        Test updating an existing block's data.
        """
        # Add a block
        self.buffer.set("TableA", 1, self.block1)
        
        # Create a new block with the same key but different data
        new_block = Block()
        new_block.header["page_id"] = 1
        new_block.add_record(b"Updated_Record1_Data")
        
        # Update the block in the buffer
        self.buffer.set("TableA", 1, new_block)
        
        # Retrieve and verify the update
        retrieved = self.buffer.get("TableA", 1)
        self.assertEqual(retrieved.header["page_id"], 1)
        self.assertEqual(retrieved.data[:retrieved.header["free_space_offset"]], b"Updated_Record1_Data")
    
    def test_lru_eviction(self):
        """
        Test that the buffer evicts the least recently used block when capacity is exceeded.
        """
        # Add blocks up to capacity
        self.buffer.set("TableA", 1, self.block1)  # MRU
        self.buffer.set("TableA", 2, self.block2)
        self.buffer.set("TableB", 1, self.block3)  # LRU
        
        # Access some blocks to change their usage order
        self.buffer.get("TableA", 1)  # Now TableA,1 is MRU
        self.buffer.get("TableA", 2)  # Now TableA,2 is MRU
        
        # Add another block, should evict TableB,1
        self.buffer.set("TableC", 1, self.block4)
        
        # Verify eviction
        evicted_block = self.buffer.get("TableB", 1)
        self.assertIsNone(evicted_block)
        
        # Verify remaining blocks
        self.assertIsNotNone(self.buffer.get("TableA", 1))
        self.assertIsNotNone(self.buffer.get("TableA", 2))
        self.assertIsNotNone(self.buffer.get("TableC", 1))
    
    def test_delete_existing_block(self):
        """
        Test deleting an existing block from the buffer.
        """
        # Add blocks
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableA", 2, self.block2)
        self.buffer.set("TableB", 1, self.block3)
        
        # Delete one block
        delete_result = self.buffer.delete("TableA", 2)
        self.assertTrue(delete_result)
        
        # Attempt to retrieve the deleted block
        deleted_block = self.buffer.get("TableA", 2)
        self.assertIsNone(deleted_block)
        
        # Ensure other blocks are still present
        self.assertIsNotNone(self.buffer.get("TableA", 1))
        self.assertIsNotNone(self.buffer.get("TableB", 1))
    
    def test_delete_nonexistent_block(self):
        """
        Test attempting to delete a block that doesn't exist in the buffer.
        """
        # Add a block
        self.buffer.set("TableA", 1, self.block1)
        
        # Attempt to delete a non-existent block
        delete_result = self.buffer.delete("TableB", 2)
        self.assertFalse(delete_result)
    
    def test_flush_buffer(self):
        """
        Test flushing (clearing) the buffer.
        """
        # Add blocks
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableA", 2, self.block2)
        self.buffer.set("TableB", 1, self.block3)
        
        # Flush the buffer
        self.buffer.flush()
        
        # Ensure all blocks are removed
        self.assertIsNone(self.buffer.get("TableA", 1))
        self.assertIsNone(self.buffer.get("TableA", 2))
        self.assertIsNone(self.buffer.get("TableB", 1))
        
        # Ensure buffer is empty
        all_blocks = self.buffer.get_all_blocks()
        self.assertEqual(len(all_blocks), 0)
    
    def test_get_all_blocks(self):
        """
        Test retrieving all blocks from the buffer.
        """
        # Add blocks
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableA", 2, self.block2)
        self.buffer.set("TableB", 1, self.block3)
        
        # Retrieve all blocks
        all_blocks = self.buffer.get_all_blocks()
        self.assertEqual(len(all_blocks), 3)
        self.assertIn(self.block1, all_blocks)
        self.assertIn(self.block2, all_blocks)
        self.assertIn(self.block3, all_blocks)
        
        # Add another block to cause eviction
        self.buffer.set("TableC", 1, self.block4)
        
        # Retrieve all blocks again
        all_blocks = self.buffer.get_all_blocks()
        self.assertEqual(len(all_blocks), 3)
        self.assertNotIn(self.block1, all_blocks)
        self.assertIn(self.block3, all_blocks)
        self.assertIn(self.block4, all_blocks)
        self.assertIn(self.block2, all_blocks)  # block2 should be evicted
    
    def test_flush_and_set(self):
        """
        Test setting new blocks after flushing the buffer.
        """
        # Add and flush blocks
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableB", 2, self.block2)
        self.buffer.flush()
        
        # Add new blocks after flush
        self.buffer.set("TableC", 3, self.block3)
        self.buffer.set("TableD", 4, self.block4)
        
        # Verify new blocks are present
        self.assertIsNotNone(self.buffer.get("TableC", 3))
        self.assertIsNotNone(self.buffer.get("TableD", 4))
        
        # Verify old blocks are not present
        self.assertIsNone(self.buffer.get("TableA", 1))
        self.assertIsNone(self.buffer.get("TableB", 2))
    
    def test_delete_after_flush(self):
        """
        Test deleting blocks after flushing the buffer.
        """
        # Add and flush blocks
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableB", 2, self.block2)
        self.buffer.flush()
        
        # Attempt to delete a block that was flushed
        delete_result = self.buffer.delete("TableA", 1)
        self.assertFalse(delete_result)
        
        # Add a new block and delete it
        self.buffer.set("TableC", 3, self.block3)
        delete_result = self.buffer.delete("TableC", 3)
        self.assertTrue(delete_result)
        self.assertIsNone(self.buffer.get("TableC", 3))
    
    def test_overwrite_and_eviction(self):
        """
        Test overwriting an existing block and subsequent eviction.
        """
        # Add three blocks
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableA", 2, self.block2)
        self.buffer.set("TableB", 1, self.block3)
        
        # Overwrite block1
        new_block1 = Block()
        new_block1.header["page_id"] = 1
        new_block1.add_record(b"Updated_Record1_Data")
        self.buffer.set("TableA", 1, new_block1)
        
        # Add another block to cause eviction
        self.buffer.set("TableC", 1, self.block4)
        
        # block2 should be evicted
        self.assertIsNone(self.buffer.get("TableA", 2))
        
        # Verify block1 is updated
        retrieved = self.buffer.get("TableA", 1)
        self.assertEqual(retrieved.data[:retrieved.header["free_space_offset"]], b"Updated_Record1_Data")
        
        # Verify other blocks are present
        self.assertIsNotNone(self.buffer.get("TableB", 1))
        self.assertIsNotNone(self.buffer.get("TableC", 1))
    
    def test_get_all_blocks_after_operations(self):
        """
        Test the state of all blocks after a series of set and get operations.
        """
        # Add three blocks
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableA", 2, self.block2)
        self.buffer.set("TableB", 1, self.block3)
        
        # Access some blocks to change usage order
        self.buffer.get("TableA", 1)  # Access block1
        self.buffer.get("TableA", 2)  # Access block2
        
        # Add another block to cause eviction
        self.buffer.set("TableC", 1, self.block4)
        
        # Retrieve all blocks
        all_blocks = self.buffer.get_all_blocks()
        self.assertEqual(len(all_blocks), 3)
        self.assertIn(self.block1, all_blocks)
        self.assertIn(self.block2, all_blocks)
        self.assertIn(self.block4, all_blocks)
        self.assertNotIn(self.block3, all_blocks)  # block3 should be evicted
    
    def test_capacity_zero_buffer(self):
        """
        Test behavior of the buffer when initialized with zero capacity.
        """
        # Initialize buffer with zero capacity
        zero_capacity_buffer = Buffer(capacity=0)
        
        # Attempt to add a block
        zero_capacity_buffer.set("TableA", 1, self.block1)
        
        # Attempt to retrieve the block (should not exist)
        retrieved = zero_capacity_buffer.get("TableA", 1)
        self.assertIsNone(retrieved)
        
        # Ensure buffer remains empty
        all_blocks = zero_capacity_buffer.get_all_blocks()
        self.assertEqual(len(all_blocks), 0)
    
    def test_add_multiple_blocks_with_same_key(self):
        """
        Test adding multiple blocks with the same key, ensuring the latest one overwrites the previous.
        """
        # Add a block
        self.buffer.set("TableA", 1, self.block1)
        
        # Add another block with the same key
        new_block = Block()
        new_block.header["page_id"] = 1
        new_block.add_record(b"New_Record1_Data")
        self.buffer.set("TableA", 1, new_block)
        
        # Retrieve and verify the latest block
        retrieved = self.buffer.get("TableA", 1)
        self.assertEqual(retrieved.header["page_id"], 1)
        self.assertEqual(retrieved.data[:retrieved.header["free_space_offset"]], b"New_Record1_Data")
        
        # Ensure only one block exists in the buffer
        all_blocks = self.buffer.get_all_blocks()
        self.assertEqual(len(all_blocks), 1)
        self.assertIn(new_block, all_blocks)
        self.assertNotIn(self.block1, all_blocks)
    
    def test_block_capacity(self):
        """
        Test that adding records beyond the block's capacity raises an error.
        """
        # Create a block and fill it to capacity
        large_record = b'A' * DATA_SIZE  # Exactly fills the data region
        block_full = Block()
        block_full.header["page_id"] = 6
        block_full.add_record(large_record)
        
        # Add the full block to the buffer
        self.buffer.set("TableD", 1, block_full)
        
        # Attempt to add another record, which should raise an error
        with self.assertRaises(ValueError):
            block_full.add_record(b"Extra_Record_Data")
    
    def test_buffer_string_representation(self):
        """
        Test the string representation of the buffer.
        """
        # Add blocks
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableB", 2, self.block2)
        
        # Get string representation
        buffer_str = str(self.buffer)
        expected_str = f"('TableB', 2): {self.block2} <-> ('TableA', 1): {self.block1}"
        self.assertEqual(buffer_str, expected_str)
    
    def test_flush_buffer_multiple_times(self):
        """
        Test flushing the buffer multiple times.
        """
        # Add blocks
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableB", 2, self.block2)
        
        # First flush
        self.buffer.flush()
        self.assertIsNone(self.buffer.get("TableA", 1))
        self.assertIsNone(self.buffer.get("TableB", 2))
        self.assertEqual(len(self.buffer.get_all_blocks()), 0)
        
        # Second flush (should still be empty)
        self.buffer.flush()
        self.assertEqual(len(self.buffer.get_all_blocks()), 0)
    
    def test_add_after_evicting_all_blocks(self):
        """
        Test adding new blocks after evicting all existing blocks due to capacity.
        """
        # Add blocks to fill the buffer
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableA", 2, self.block2)
        self.buffer.set("TableB", 1, self.block3)
        
        # Add additional blocks to evict all existing blocks
        self.buffer.set("TableC", 1, self.block4)
        self.buffer.set("TableD", 2, self.block5)
        
        # Verify evictions
        self.assertIsNone(self.buffer.get("TableA", 1))
        self.assertIsNone(self.buffer.get("TableA", 2))

        
        # Verify new blocks are present
        self.assertIsNotNone(self.buffer.get("TableB", 1))
        self.assertIsNotNone(self.buffer.get("TableC", 1))
        self.assertIsNotNone(self.buffer.get("TableD", 2))
    
    def test_get_all_blocks_after_flush_and_set(self):
        """
        Test retrieving all blocks after flushing and setting new blocks.
        """
        # Add blocks and flush
        self.buffer.set("TableA", 1, self.block1)
        self.buffer.set("TableB", 2, self.block2)
        self.buffer.flush()
        
        # Add new blocks
        self.buffer.set("TableC", 3, self.block3)
        self.buffer.set("TableD", 4, self.block4)
        
        # Retrieve all blocks
        all_blocks = self.buffer.get_all_blocks()
        self.assertEqual(len(all_blocks), 2)
        self.assertIn(self.block3, all_blocks)
        self.assertIn(self.block4, all_blocks)
        self.assertNotIn(self.block1, all_blocks)
        self.assertNotIn(self.block2, all_blocks)

if __name__ == "__main__":
    unittest.main(testRunner=ColoredTextTestRunner())