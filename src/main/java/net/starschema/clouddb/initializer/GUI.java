/**
 * Copyright (c) 2015, STARSCHEMA LTD.
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:

 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This class implements the java.sql.Connection interface
 */

package net.starschema.clouddb.initializer;

import java.awt.EventQueue;
import java.awt.GridLayout;
import java.awt.Image;

import javax.imageio.ImageIO;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.EmptyBorder;
import javax.swing.filechooser.FileFilter;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import javax.swing.JCheckBox;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.JScrollPane;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;

/**
 * A GUI application for the Starschema JDBC Driver, for testing
 * the connection and/or run queries against bigQuery.
 *
 * @author Balazs Gunics
 *
 */
public class GUI extends JFrame {

    private static final long serialVersionUID = 1L;
    private static String st_TestConnection = "Test Connection";

    private static String st_ProjectId = "Obtained from https://code.google.com/apis/console";
    private static String st_Pass = "Client secret";
    private static String st_UserName = "Client ID";
    private static String st_DriverPath = "path to the downloaded driver";
    private String jdbcUrl = "jdbc:BQDriver:";

    private JPanel contentPane;

    private JLabel lblInfoText;
    private JCheckBox checkBoxQTE;
    private JCheckBox checkBoxSA;
    private JCheckBox checkBoxIncludeUserPass;

    private JPanel j1;
    private JPanel j2;

    private JLabel tf_DriverPath;
    private JTextField tf_userName;
    private JTextField tf_pass;
    private JTextField tf_projectId;
    private JTextField label_jdbcURL_output;

    private JButton button_testConnection;
    private JTextArea queryText;
    private JTable table;
    private JScrollPane scrollPane;
    private JTextField txtBQDriver;

    /**
     * Launch the application.
     */
    public static void main(String[] args) {
        EventQueue.invokeLater(new Runnable() {
            public void run() {
                try {
                    GUI frame = new GUI();
                    frame.setVisible(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * Create the frame.
     */
    public GUI() {
        setResizable(false);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setBounds(100, 100, 909, 650);
        contentPane = new JPanel();
        contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));

        setContentPane(contentPane);
        setTitle("Starschema JDBC initializer");
        contentPane.setLayout(null);
        j1 = new JPanel();
        j1.setBounds(5, 5, 878, 220);
        j1.setLayout(null);
        JLabel label_path = new JLabel("JDBC Driver path:");
        label_path.setBounds(10, 10, 99, 22);
        j1.add(label_path);
        getContentPane().add(j1);
        JLabel lblUsernameclientId = new JLabel("Username (Client ID):");
        lblUsernameclientId.setBounds(10, 38, 200, 22);
        j1.add(lblUsernameclientId);
        JLabel lblPasswordclientSecret = new JLabel("Password (Client secret):");
        lblPasswordclientSecret.setBounds(10, 68, 200, 22);
        j1.add(lblPasswordclientSecret);
        JLabel lblProjectId = new JLabel("Project ID:");
        lblProjectId.setBounds(10, 96, 200, 22);
        j1.add(lblProjectId);
        tf_DriverPath = new JLabel(st_DriverPath);
        tf_DriverPath.setBounds(234, 10, 380, 22);
        j1.add(tf_DriverPath);
        j1.add(tf_userName = new JTextField(st_UserName));
        tf_userName.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent e) {
                tf_userName.selectAll();
            }
        });
        tf_userName.setBounds(234, 38, 380, 22);
        j1.add(tf_userName);
        j1.add(tf_pass = new JTextField(st_Pass));
        tf_pass.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent e) {
                tf_pass.selectAll();
            }
        });
        tf_pass.setBounds(234, 68, 380, 22);
        j1.add(tf_pass);
        j1.add(tf_projectId = new JTextField(st_ProjectId));
        tf_projectId.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent e) {
                tf_projectId.selectAll();
            }
        });
        tf_projectId.setBounds(234, 96, 380, 22);
        j1.add(tf_projectId);

        //checkboxes
        checkBoxSA = new JCheckBox("Connect with service account");
        checkBoxSA.setBounds(620, 68, 240, 22);
        j1.add(checkBoxSA);
        checkBoxQTE = new JCheckBox("Enable Query Transformation Engine");
        checkBoxQTE.setBounds(620, 10, 250, 22);
        j1.add(checkBoxQTE);

        JLabel label_jdbcURL_text = new JLabel("The JDBC Url for the connection is:");
        label_jdbcURL_text.setBounds(10, 152, 200, 22);
        j1.add(label_jdbcURL_text);
        label_jdbcURL_output = new JTextField(jdbcUrl);
        label_jdbcURL_output.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent e) {
                label_jdbcURL_output.selectAll();
            }
        });
        label_jdbcURL_output.setEditable(false);
        label_jdbcURL_output.setBounds(234, 152, 604, 22);
        j1.add(label_jdbcURL_output);

        //adding the Button to the main panel
        JButton btnGetJdbcUrl = new JButton("Get JDBC url");
        //adding listener to the button
        btnGetJdbcUrl.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent arg0) {
                ConstructJDBC();
            }
        });
        btnGetJdbcUrl.setBounds(10, 183, 200, 29);
        j1.add(btnGetJdbcUrl);

        button_testConnection = new JButton(st_TestConnection);
        button_testConnection.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent arg0) {
                TestConnection();
            }
        });
        button_testConnection.setBounds(220, 183, 200, 29);
        j1.add(button_testConnection);

        checkBoxIncludeUserPass = new JCheckBox("Include Username and Password");
        checkBoxIncludeUserPass.setBounds(620, 38, 331, 23);
        j1.add(checkBoxIncludeUserPass);

        JButton button_OpenDriver = new JButton("Open/Load");
        button_OpenDriver.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent arg0) {
                OpenDriver();
            }
        });
        button_OpenDriver.setBounds(120, 10, 100, 23);
        j1.add(button_OpenDriver);

        lblInfoText = new JLabel("");
        lblInfoText.setBounds(430, 183, 430, 29);
        j1.add(lblInfoText);

        JLabel lblJDBCDriverName = new JLabel("JDBC Drivers Class Name");
        lblJDBCDriverName.setBounds(10, 129, 200, 14);
        j1.add(lblJDBCDriverName);

        txtBQDriver = new JTextField();
        txtBQDriver.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent arg0) {
                txtBQDriver.selectAll();
            }
        });
        txtBQDriver.setEditable(false);
        txtBQDriver.setText("net.starschema.clouddb.jdbc.BQDriver");
        txtBQDriver.setBounds(234, 124, 380, 20);
        j1.add(txtBQDriver);
        txtBQDriver.setColumns(10);

        JPanel panel = new JPanel();
        panel.setBounds(5, 236, 878, 365);
        contentPane.add(panel);
        panel.setLayout(null);

        JButton btnNewButton = new JButton("Run Query");
        btnNewButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent arg0) {
                runQuery();
            }
        });
        btnNewButton.setBounds(480, 0, 104, 32);
        panel.add(btnNewButton);

        JLabel lblDriverTesterV = new JLabel("Driver Tester v1, if you want to use publicdata: turn the Query Transform off.");
        lblDriverTesterV.setBounds(10, 9, 604, 14);
        panel.add(lblDriverTesterV);

        queryText = new JTextArea();
        queryText.setBounds(10, 35, 858, 100);
        panel.add(queryText);
        queryText.setColumns(10);

        scrollPane = new JScrollPane();
        scrollPane.setBounds(10, 146, 858, 208);

        table = new JTable();
        scrollPane.setViewportView(table);
        panel.add(scrollPane);

        j2 = new JPanel();
        j2.setBounds(5, 281, 624, 176);
        j2.setLayout(new GridLayout(0, 2));


        InputStream input = this.getClass().getClassLoader()
                .getResourceAsStream("starlogo.png");
        Image img = null;
        try {
            img = ImageIO.read(input);
        } catch (IOException e) {
            //should not happen
        }
        this.setIconImage(img);
    }

    /**
     * To check the user and password could be valid
     * <p>
     * <li>if the username doesn't end with "apps.googleusercontent.com" it can't be valid
     * <li>if we want to connect with service account the password must be valid path
     * <li>in Oauth password doesn't contains special characters or space
     * </p>
     * @return false if we couldn't make a valid connection with these Parameters
     */
    private boolean validParameters() {
        String sequence = ".,;:_?";

        if (tf_userName.getText().contains(" ")) {
            lblInfoText.setText("UserName can't contain \"space\"");
            return false;
        }

        if (tf_userName.getText()
                .endsWith(".apps.googleusercontent.com") == false) {
            lblInfoText.setText("Username must end with \".apps.googleusercontent.com\"");
            return false;
        }
        if (checkBoxSA.isSelected()) {
            //service account - pass must be a valid path to the keyfile!

            File file = new File(tf_pass.getText());
            if (!file.exists()) {
                lblInfoText.setText("Password must be a valid path to the keyfile!");
                return false;
            }
        } else {
            for (char myChar : tf_pass.getText().toCharArray()) {
                String myCharString = String.valueOf(myChar);

                if (sequence.contains(myCharString)) {
                    lblInfoText.setText("password can't contain: " + sequence);
                    return false;
                }
            }
            if (tf_pass.getText().contains(" ")) {
                lblInfoText.setText("password can't contain \"space\"");
                return false;
            }
        }

        //checking the project ID
        if (tf_projectId.getText().contains(" ")) {
            lblInfoText.setText("Project Id can't contain \"space\"");
            return false;
        }
        lblInfoText.setText("");
        return true;
    }

    /**
     * To Construct the JDBC url
     */
    private void ConstructJDBC() {
        if (!validParameters()) return;
        jdbcUrl = "jdbc:BQDriver:";
        jdbcUrl += tf_projectId.getText().replace(":", "%3A");
        if (checkBoxQTE.isSelected()) {
            jdbcUrl += "?transformQuery=true";
        }
        if (checkBoxSA.isSelected()) {
            jdbcUrl += "?withServiceAccount=true";
        }
        if (checkBoxIncludeUserPass.isSelected()) {
            jdbcUrl += "?user=" + tf_userName.getText();
            jdbcUrl += "?password=" + tf_pass.getText();
        }
        label_jdbcURL_output.setText(jdbcUrl);
    }

    private void OpenDriver() {
        JFileChooser fileDialog = new JFileChooser();

        ExtensionFileFilter jarFilter = new ExtensionFileFilter(
                new String("jar"), "Jar Files");
        for (FileFilter filefilter : fileDialog.getChoosableFileFilters()) {
            fileDialog.removeChoosableFileFilter(filefilter);
        }
        fileDialog.addChoosableFileFilter(jarFilter);

/*		String defaultPath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
		System.out.println("default path:" + defaultPath);*/
        try {
            fileDialog.setCurrentDirectory(
                    new File(this.getClass().getProtectionDomain()
                            .getCodeSource().getLocation().toURI().getPath())
            );
        } catch (URISyntaxException e) {
            //do nothing, since this is just a help
        }
        File defaultDriver = new File(this.getClass().getProtectionDomain()
                .getCodeSource().getLocation().getFile());
		/*if(defaultDriver.exists()) {
		    tf_DriverPath.setText(defaultDriver.getAbsolutePath());
		    return;
		}
		*/
        fileDialog.setSelectedFile(defaultDriver);

        int option = fileDialog.showOpenDialog(GUI.this);
        if (option == JFileChooser.APPROVE_OPTION) {
            tf_DriverPath.setText(
                    fileDialog.getSelectedFile() != null ?
                            fileDialog.getSelectedFile().getPath() : st_DriverPath
            );
        } else tf_DriverPath.setText(st_DriverPath);
    }

    private void TestConnection() {
        if (!validParameters()) return;
        final String samplesql = "SELECT corpus, corpus_date FROM publicdata:samples.shakespeare " +
                "GROUP BY corpus, corpus_date ORDER BY corpus_date DESC LIMIT 1";
        Thread runquery = new Thread(
                new Runnable() {
                    public void run() {
                        connectAndRunQuery(false
                                , samplesql);
                    }
                }
        );
        runquery.start();
        lblInfoText.setText("Connection testing...");
    }

    private void runQuery() {
        if (!validParameters()) return;
        Thread runquery = new Thread(
                new Runnable() {
                    public void run() {
                        connectAndRunQuery(
                                checkBoxQTE.isSelected()
                                , queryText.getText());
                    }
                });
        runquery.start();
    }

    /**
     *
     */
    private void connectAndRunQuery(boolean QTE, String query) {
        Connection con = null;

        try {
            URL[] myJars = new URL[1];
            myJars[0] = new URL("file:///" + tf_DriverPath.getText());
            String classname = "net.starschema.clouddb.jdbc.BQDriver";
            URLClassLoader child = new URLClassLoader(myJars
                    , this.getClass().getClassLoader());
            Driver d = (Driver) Class.forName(classname
                    , true, child).newInstance();
            DriverManager.registerDriver(new DriverShim(d));

            String URL = "jdbc:BQDriver:"
                    + tf_projectId.getText().replace(":", "%3A");
            if (QTE) URL += "?transformQuery=true";
            con = DriverManager.getConnection(URL, tf_userName.getText()
                    , tf_pass.getText());
        } catch (Exception e) {
            lblInfoText.setText("We failed at making connection " + e.getLocalizedMessage());
            return;
        }
        ResultSet myResult = null;

        try {
            myResult = con.createStatement().executeQuery(query);
        } catch (SQLException e) {
            lblInfoText.setText("We failed at running statement connection " + e.getLocalizedMessage());
            return;
        }
        try {
            if (myResult.last()) {
                lblInfoText.setText("Connection was successfull");
                table.setModel(DbUtils.resultSetToTableModel(myResult));
                table.setVisible(true);
                table.setAutoscrolls(true);
            }
        } catch (SQLException e) {
            lblInfoText.setText("No results " + e.getLocalizedMessage());
            return;
        } finally {
            try {
                myResult.close();
                myResult = null;
            } catch (SQLException se) {
                //do nothing
                myResult = null;
            }
        }

    } //end of connect
}
