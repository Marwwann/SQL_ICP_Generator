from PyQt5.QtWidgets import QFileDialog, QWidget, QVBoxLayout, QApplication, QPushButton, QLabel

class FolderSelection(QWidget):
    def __init__(self):
        super().__init__()

        self.button = QPushButton("Select Folder")
        self.button.clicked.connect(self.get_folder_path)

        self.folder_path_label = QLabel("No folder selected yet.")

        layout = QVBoxLayout()
        layout.addWidget(self.button)
        layout.addWidget(self.folder_path_label)
        self.setLayout(layout)

    def get_folder_path(self):
        """
        Opens a file dialog to select a folder and displays the selected path.
        """
        folder_path = QFileDialog.getExistingDirectory(self, "Select Folder")
        if folder_path:
            self.folder_path_label.setText(f"Selected Folder: {folder_path}")
        else:
            print("No folder selected.")

if __name__ == "__main__":
    import sys
    app = QApplication(sys.argv)
    window = FolderSelection()
    window.show()
    sys.exit(app.exec_())