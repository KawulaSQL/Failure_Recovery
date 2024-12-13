from typing import List
from Storage_Manager.lib.Block import Block

class DoublyLinkedListNode:
    def __init__(self, key: tuple, value: Block):
        self.key = key       
        self.value = value     
        self.prev = None      
        self.next = None        

class DoublyLinkedList:
    def __init__(self):
        self.head = DoublyLinkedListNode(None, None) 
        self.tail = DoublyLinkedListNode(None, None) 
        self.head.next = self.tail
        self.tail.prev = self.head

    def add_to_front(self, node: DoublyLinkedListNode):
        """Add node right after head."""
        node.prev = self.head
        node.next = self.head.next
        self.head.next.prev = node
        self.head.next = node

    def remove_node(self, node: DoublyLinkedListNode):
        """Remove an existing node from the list."""
        prev_node = node.prev
        next_node = node.next
        prev_node.next = next_node
        next_node.prev = prev_node

    def move_to_front(self, node: DoublyLinkedListNode):
        """Move a node to the front (right after head)."""
        self.remove_node(node)
        self.add_to_front(node)

    def remove_from_end(self) -> DoublyLinkedListNode:
        """Remove and return the node just before tail (least recently used)."""
        if self.tail.prev == self.head:
            return None  # List is empty
        node = self.tail.prev
        self.remove_node(node)
        return node


class Buffer:
    def __init__(self, capacity: int):
        """
        Initialize the Buffer with a given capacity.

        :param capacity: Maximum number of blocks the buffer can hold.
        """
        self.capacity = capacity
        self.cache = {}  # Maps key (table_name, offset) to DoublyLinkedListNode
        self.dll = DoublyLinkedList()

    def get(self, table_name: str, offset: int) -> Block:
        """
        Retrieve a block from the buffer.

        :param table_name: Name of the table.
        :param offset: Offset of the block within the table.
        :return: The requested Block if found, else None.
        """
        key = (table_name, offset)
        node = self.cache.get(key, None)
        if not node:
            return None
        # Move the accessed node to the front (most recently used)
        self.dll.move_to_front(node)
        return node.value

    def set(self, table_name: str, offset: int, block: Block):
        """
        Add or update a block in the buffer.

        :param table_name: Name of the table.
        :param offset: Offset of the block within the table.
        :param block: The Block to be stored.
        """
        key = (table_name, offset)
        node = self.cache.get(key, None)

        if node:
            # Update the value and move it to the front
            node.value = block
            self.dll.move_to_front(node)
        else:
            # Add new node to the front
            new_node = DoublyLinkedListNode(key, block)
            self.dll.add_to_front(new_node)
            self.cache[key] = new_node

            if len(self.cache) > self.capacity:
                # Remove the least recently used block
                lru_node = self.dll.remove_from_end()
                if lru_node:
                    del self.cache[lru_node.key]

    def flush(self):
        """
        Write all the blocks in the buffer to disk.

        Each block is written to a file based on its table name and offset.
        File path: ../Storage_Manager/storage/{table_name}_table.bin
        """
        current_node = self.dll.head.next 

        while current_node != self.dll.tail: 
            key = current_node.key  
            table_name, offset = key
            block = current_node.value 

            file_path = f"../Storage_Manager/storage/{table_name}_table.bin"
            if (block.header["free_space_offset"]  != 0):
                block.write_block(file_path, offset)

            current_node = current_node.next

        self.cache.clear()
        # Reset the doubly linked list
        self.dll = DoublyLinkedList()

    def delete(self, table_name: str, offset: int) -> bool:
        """
        Delete a specific block from the buffer.

        :param table_name: Name of the table.
        :param offset: Offset of the block within the table.
        :return: True if the block was found and deleted, False otherwise.
        """
        key = (table_name, offset)
        node = self.cache.get(key, None)
        if not node:
            return False
        self.dll.remove_node(node)
        del self.cache[key]
        return True

    def get_all_blocks(self) -> List[Block]:
        """
        Retrieve all blocks currently stored in the buffer.

        :return: List of all Block objects in the buffer.
        """
        blocks = []
        current = self.dll.head.next
        while current != self.dll.tail:
            blocks.append(current.value)
            current = current.next
        return blocks
    def __str__(self):
        """
        For debugging purposes: Returns a string representation of the buffer's current state.
        """
        nodes = []
        current = self.dll.head.next
        while current != self.dll.tail:
            nodes.append(f"{current.key}: {current.value}")
            current = current.next
        return " <-> ".join(nodes)
