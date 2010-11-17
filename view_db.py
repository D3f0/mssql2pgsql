'''
Created on 28/07/2010

@author: defo
'''
import sys
from PyQt4.Qt import *



class VentanaDB(QWidget):
    def __init__(self, parent = None):
        QWidget.__init__(self, parent)
        self.setupUi()
    
    def setupUi(self):
        layout = QVBoxLayout()
        listado = QListWidget()
        layout.addWidget(listado)
        
        btn = QPushButton("Hola")
        self.connect(btn, SIGNAL('pressed()'), self.close)
        layout.addWidget(btn)
        self.setLayout(layout)






def main(argv = sys.argv):
    app = QApplication(argv)
    win= VentanaDB()
    win.setWindowTitle("Prueba de DB")
    win.show()
    return app.exec_()
    
if __name__ == "__main__":
    sys.exit(main())