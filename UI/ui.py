from PyQt5.QtWidgets import QApplication, QMainWindow, QListWidgetItem, QFileDialog
from PyQt5.uic import loadUi
from PyQt5 import QtCore, Qt
import helper
from main import generateScripts
import sys


class GenerateWindow(QMainWindow):
    def __init__(self, main_obj):
        super(GenerateWindow, self).__init__()
        loadUi("generateWindow.ui", self)
        self.generateButton.setDisabled(True)
        self.browseButton.clicked.connect(self.browseFn)
        self.generateButton.clicked.connect(self.generateButtonFn)
        self.main_obj = main_obj
        self.url = self.main_obj.url
        self.username = self.main_obj.username
        self.password = self.main_obj.password
        self.elements_list = self.main_obj.required_labels
        self.total_string = "".join(["'"+label+"', " for label in self.main_obj.required_labels])

    def browseFn(self):
        self.folder_path = QFileDialog.getExistingDirectory(self, "Browse")
        if self.folder_path:
            self.destinationLineEdit.setText(self.folder_path)
            self.destinationLineEdit.setDisabled(True)
            self.generateButton.setEnabled(True)

    def generateButtonFn(self):
        generateScripts(save_directory= self.folder_path, instance=self.url, username=self.username,
                        password=self.password, total_string=self.total_string, elements_list=self.elements_list)
class Main(QMainWindow):
    def __init__(self):
        super(Main, self).__init__()
        loadUi("main.ui", self)
        self.main_elements_list = []
        self.checked_list = []
        self.match_elements = []
        self.required_labels = []
        self.url = ""
        self.username = ""
        self.password = ""
        self.editButton.setDisabled(True)
        self.searchLineEdit.setDisabled(True)
        self.initiateButton.clicked.connect(self.initate)
        self.nextButton.clicked.connect(self.generateWindowFn)
        self.searchLineEdit.textChanged.connect(self.Search)

    def initate(self):
        self.url = self.instanceLineEdit.text()
        self.username = self.usernameLineEdit.text()
        self.password = self.passwordLineEdit.text()

        result = helper.validateAuthentication(self.url, self.username, self.password)
        print(result)
        if result == '':
            elements_list = helper.getElementTypes(self.url, self.username, self.password)
            self.main_elements_list = elements_list
            self.editButton.setEnabled(True)
            self.searchLineEdit.setEnabled(True)
            self.instanceLineEdit.setDisabled(True)
            self.usernameLineEdit.setDisabled(True)
            self.passwordLineEdit.setDisabled(True)
            self.initiateButton.setDisabled(True)
            for element, element_id in elements_list:
                item = QListWidgetItem(element)
                item.setData(QtCore.Qt.UserRole, element_id)
                item.setFlags(item.flags() | QtCore.Qt.ItemIsUserCheckable)
                item.setCheckState(QtCore.Qt.Unchecked)
                self.listWidget.addItem(item)

    def Search(self, text):
        if text == '':
            self.getCheckedIcp()
            self.listWidget.clear()
            for element, element_id in self.main_elements_list:
                item = QListWidgetItem(element)
                item.setData(QtCore.Qt.UserRole, element_id)
                item.setFlags(item.flags() | QtCore.Qt.ItemIsUserCheckable)
                if element_id in self.checked_list:
                    item.setCheckState(QtCore.Qt.Checked)
                else:
                    item.setCheckState(QtCore.Qt.Unchecked)
                self.listWidget.addItem(item)
        else:
            self.getCheckedIcp()
            match_elements = []
            for matched_element, matched_element_id in self.main_elements_list:
                if matched_element is None:
                    continue
                elif text in matched_element:
                    match_elements.append((matched_element,matched_element_id))
            self.listWidget.clear()
            for match_element, match_element_id in match_elements:
                item = QListWidgetItem(match_element)
                item.setData(QtCore.Qt.UserRole, match_element_id)
                item.setFlags(item.flags() | QtCore.Qt.ItemIsUserCheckable)
                if match_element_id in self.checked_list:
                    item.setCheckState(QtCore.Qt.Checked)
                else:
                    item.setCheckState(QtCore.Qt.Unchecked)
                self.listWidget.addItem(item)


    def getCheckedIcp(self):
        for i in range(self.listWidget.count()):
            item = self.listWidget.item(i)
            if item.checkState() == QtCore.Qt.Checked:
                self.checked_list.append(item.data(QtCore.Qt.UserRole))


    def generateWindowFn(self):
        self.getCheckedIcp()
        self.unique_checked_list = list(set(self.checked_list))
        text = ""
        for element, element_id in self.main_elements_list:
            if element_id in self.unique_checked_list:
                self.required_labels.append(element)
        self.generate_window = GenerateWindow(window)
        for label in self.required_labels:
            text += "• " + label + "\r\n"
        text = text[:-2]
        self.generate_window.icpLabel.setText(text)
        self.generate_window.show()



if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = Main()
    window.show()
    app.exec_()