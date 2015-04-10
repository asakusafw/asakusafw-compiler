/**
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.lang.compiler.inspection.gui;

import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;

import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.inspection.InspectionNode;
import com.asakusafw.lang.compiler.inspection.InspectionNodeRepository;
import com.asakusafw.lang.compiler.inspection.json.JsonInspectionNodeRepository;

/**
 * Program entry for inspection tools.
 */
public class Main implements Runnable {

    static final Logger LOG = LoggerFactory.getLogger(Main.class);

    final InspectionNodeRepository repository;

    final PdfExporter pdfExporter;

    File selectedFile;

    File lastOutput;

    /**
     * Creates a new instance.
     */
    public Main() {
        this(new JsonInspectionNodeRepository(), PdfExporter.find());
    }

    /**
     * Creates a new instance.
     * @param repository the node repository
     * @param pdfExporter the PDF exporter
     */
    public Main(InspectionNodeRepository repository, PdfExporter pdfExporter) {
        this.repository = repository;
        this.pdfExporter = pdfExporter;
    }

    /**
     * Program entry.
     * @param args program arguments (ignored)
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Main());
    }

    @Override
    public void run() {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception e) {
            LOG.warn("failed to switch look and feel", e);
        }

        JFrame frame = new JFrame("inspection");
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.setSize(400, 300);
        frame.setVisible(true);

        JFileChooser chooser = new JFileChooser();
        chooser.setMultiSelectionEnabled(false);
        int result = chooser.showOpenDialog(frame.getContentPane());
        if (result != JFileChooser.APPROVE_OPTION) {
            frame.dispose();
            return;
        }

        selectedFile = chooser.getSelectedFile();
        InspectionNode node;
        try {
            node = loadFile(selectedFile);
        } catch (IOException e) {
            JOptionPane.showMessageDialog(frame.getContentPane(),
                    MessageFormat.format(
                            "failed to load inspection file: {0}",
                            selectedFile),
                    "Error",
                    JOptionPane.ERROR_MESSAGE);
            frame.dispose();
            return;
        }

        frame.setTitle(MessageFormat.format("{1} [@{0}]", node.getId(), node.getTitle()));
        JTree tree = createOverviewTree(node);
        frame.getContentPane().add(new JScrollPane(tree));

        frame.revalidate();
    }

    private InspectionNode loadFile(File file) throws IOException {
        try (InputStream input = new FileInputStream(file)) {
            return repository.load(input);
        }
    }

    private JTree createOverviewTree(InspectionNode root) {
        final JTree tree = new JTree(new InspectionTreeNode(root));
        tree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
        tree.addMouseListener(new MouseAdapter() {
            @Override
            public void mousePressed(MouseEvent e) {
                if (e.isPopupTrigger()) {
                    popup(e);
                }
            }

            @Override
            public void mouseReleased(MouseEvent e) {
                if (e.isPopupTrigger()) {
                    popup(e);
                }
            }

            private void popup(MouseEvent event) {
                TreePath path = tree.getPathForLocation(event.getX(), event.getY());
                if (path == null) {
                    return;
                }
                final InspectionTreeNode node = (InspectionTreeNode) path.getLastPathComponent();
                JPopupMenu menu = new JPopupMenu();
                menu.add(item("Open Detail", new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        onOpenDetail(tree, node);
                    }
                }));
                if (pdfExporter != null && node.isLeaf() == false) {
                    menu.add(item("Export PDF", new ActionListener() {
                        @Override
                        public void actionPerformed(ActionEvent e) {
                            onExportPdf(tree, node, false);
                        }
                    }));
                    menu.add(item("Export PDF (Verbose)", new ActionListener() {
                        @Override
                        public void actionPerformed(ActionEvent e) {
                            onExportPdf(tree, node, true);
                        }
                    }));
                }
                menu.show(tree, event.getX(), event.getY());
            }
        });
        return tree;
    }

    void onOpenDetail(JTree parent, InspectionTreeNode treeNode) {
        InspectionNode node = treeNode.getUserObject();

        JDialog dialog = new JDialog(SwingUtilities.getWindowAncestor(parent));
        dialog.setTitle(String.format("%s [@%s]", node.getTitle(), node.getId()));
        dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        dialog.setSize(400, 300);

        Window ancestor = SwingUtilities.getWindowAncestor(parent);
        dialog.setLocation(ancestor.getLocation());

        JTree tree = createDetailTree(treeNode);
        dialog.getContentPane().add(new JScrollPane(tree));
        dialog.setVisible(true);
    }

    private JTree createDetailTree(InspectionTreeNode treeNode) {
        JTree tree = new JTree(new DetailTreeNode(treeNode.getUserObject()));
        tree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
        return tree;
    }

    void onExportPdf(JTree parent, InspectionTreeNode node, boolean verbose) {
        JFileChooser chooser = new JFileChooser();
        chooser.setMultiSelectionEnabled(false);
        if (lastOutput == null) {
            chooser.setCurrentDirectory(selectedFile.getParentFile());
        } else {
            chooser.setSelectedFile(lastOutput);
        }
        int result = chooser.showSaveDialog(parent);
        if (result != JFileChooser.APPROVE_OPTION) {
            return;
        }
        File file = chooser.getSelectedFile();
        lastOutput = file;
        try {
            pdfExporter.export(file, node.getUserObject(), verbose);
        } catch (IOException | InterruptedException e) {
            LOG.error(MessageFormat.format(
                    "failed to export PDF file",
                    file), e);
            JOptionPane.showMessageDialog(parent,
                    MessageFormat.format(
                            "failed to export PDF file: {0} ({1})",
                            file, e.toString()),
                    "Error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }
        JOptionPane.showMessageDialog(parent, MessageFormat.format(
                "exported PDF file: {0}",
                file));
    }

    JMenuItem item(String title, ActionListener listener) {
        JMenuItem item = new JMenuItem(title);
        item.addActionListener(listener);
        return item;
    }
}

