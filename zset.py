__author__ = 'Ma Haoxiang'

class TreeNode:
    '''
        basic class of tree node in a binary search tree
    '''
    def __init__(self, value, score):
        self.value = value
        self.score = score
        self.left = None
        self.right = None

class BSTree:
    '''
        basic class of a binary search tree (BST)
    '''
    def __init__(self):
        self.root = None

    def insert(self, value, score):
        newNode = TreeNode(value, score)
        if(self.root is None):
            self.root = newNode
        else:
            current = self.root
            parent = self.root
            while(True):
                parent = current
                if(current.score <= score):
                    current = current.right
                    if(current is None):
                        parent.right = newNode
                        break
                else:
                    current = current.left
                    if(current is None):
                        parent.left = newNode
                        break

    def find(self, key):
        result = self.inOrderSearch(self.root, key)
        return result

    def inOrderSearch(self, current, key):
        if(current is not None):
            result = self.inOrderSearch(current.left, key)
            if(result is not None):
                return result
            if(current.value == key):
                return current
            result = self.inOrderSearch(current.right, key)
            if (result is not None):
                return result
        return None

    def findMin(self):
        current = self.root
        parent = self.root
        while(current is not None):
            parent = current
            current = current.left
        return parent

    def findMax(self):
        current = self.root
        parent = self.root
        while(current is not None):
            parent = current
            current = current.right
        return parent

    '''
        public interface for in-order traverse
        When you need to save the traverse result, set traverseResult as a list,
        then the result will be appended into the list
    '''
    def inOrder(self, traverseResult=None):
        self.internalInOrder(self.root, traverseResult)

    '''
        internal function for in-order traverse
    '''
    def internalInOrder(self, current, traverseResult=None):
        if(current is not None):
            self.internalInOrder(current.left, traverseResult)
            if(traverseResult is not None):
                traverseResult.append((current.value, current.score))
            else:
                print (current.value, current.score)
            self.internalInOrder(current.right, traverseResult)

    '''
        a delete function for binary search tree
    '''
    def delete(self, key):
        if(self.root is None):
            return False
        result = self.find(key)
        if(result is None):
            return False
        score = result.score

        current = self.root
        parent = self.root
        while(current.score != score):
            parent = current
            if(current.score < score):
                current = current.right
            else:
                current = current.left

        # case1 leaf node
        if(current.left is None and current.right is None):
            if(current == self.root):
                self.root = None
            else:
                if(current == parent.left):
                    parent.left = None
                else:
                    parent.right = None

        # case2 del node has a left child
        elif(current.left is not None and current.right is None):
            if(current == self.root):
                self.root = current.left
            else:
                if(current == parent.left):
                    parent.left = current.left
                else:
                    parent.right = current.left

        # case3 del node has a right child
        elif(current.left is None and current.right is not None):
            if(current == self.root):
                self.root = current.right
            else:
                if(current == parent.left):
                    parent.left = current.right
                else:
                    parent.right = current.right

        # case4 del node has two children
        elif(current.left is not None and current.right is not None):
            successor = self.getSuccessor(current)
            if(current == self.root):
                self.root = successor
            else:
                if(current == parent.left):
                    parent.left = successor
                else:
                    parent.right = successor
            successor.left = current.left
        return True

    # function to get the direct successor of current node
    def getSuccessor(self, delNode):
        successor = delNode
        successorParent = delNode
        current = delNode.right

        while(current is not None):
            successorParent = successor
            successor = current
            current = current.left

        if(successor != delNode.right):
            successorParent.left = successor.right
            successor.right = delNode.right
        return successor

class zset:
    '''
        a class of sorted set (implemented by binary search tree)
    '''
    def __init__(self):
        self.BSTree = BSTree()
        self.valueDict = {}

    def add(self, value, score):
        if(value in self.valueDict.keys()):
            return False
        else:
            self.BSTree.insert(value, score)
            self.valueDict[value] = score
            return True

    def find(self, key):
        try:
            return (key, self.valueDict[key])
        except:
            return (key, None)

    def findMin(self):
        result = self.BSTree.findMin()
        if(result is None):
            return (None, None)
        else:
            return (result.value, result.score)

    def findMax(self):
        result = self.BSTree.findMax()
        if(result is None):
            return (None, None)
        else:
            return (result.value, result.score)

    def remove(self, key):
        result = self.BSTree.delete(key)
        if(result is True):
            self.valueDict.pop(key)
        return result

    def display(self):
        self.BSTree.inOrder()

    def get(self):
        traverseResult = []
        self.BSTree.inOrder(traverseResult)
        return traverseResult



if __name__ == "__main__":
    pass
