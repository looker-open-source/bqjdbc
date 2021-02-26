package com.google.api.services.bigquery;

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.GetIamPolicyRequest;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.Model;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.Routine;
import com.google.api.services.bigquery.model.SetIamPolicyRequest;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TestIamPermissionsRequest;

import java.io.IOException;

/**
 * A small proxy class with which every request is by default minified JSON.
 * This is to help improve performance for BigQuery as for certain payloads the cost of whitespace
 * can be quite taxing.
 */
public class MinifiedBigquery extends Bigquery{

    /**
     * By default we minify (non-pretty) the JSON.
     * We leave it toggleable for debugging purposes.
     */
    private boolean pretty;
    
    /**
     * The way in which we create our minified version.
     * Create a normal builder but don't call build but pass it to this constructor.
     * Defaults the pretty printing to false.
     */
    public MinifiedBigquery(Builder builder) {
        this(builder, false);
    }

    public MinifiedBigquery(Builder builder, boolean pretty) {
        super(builder);
        this.pretty = pretty;
    }
    
    @Override
    protected void initialize(AbstractGoogleClientRequest<?> httpClientRequest) throws IOException {
        super.initialize(httpClientRequest);
    }

    @Override
    public Datasets datasets() {
        return new Datasets() {
            @Override
            public Delete delete(String projectId, String datasetId) throws IOException {
                return super.delete(projectId, datasetId).setPrettyPrint(pretty);
            }

            @Override
            public Get get(String projectId, String datasetId) throws IOException {
                return super.get(projectId, datasetId).setPrettyPrint(pretty);
            }

            @Override
            public Insert insert(String projectId, Dataset content) throws IOException {
                return super.insert(projectId, content).setPrettyPrint(pretty);
            }

            @Override
            public List list(String projectId) throws IOException {
                return super.list(projectId).setPrettyPrint(pretty);
            }

            @Override
            public Patch patch(String projectId, String datasetId, Dataset content) throws IOException {
                return super.patch(projectId, datasetId, content).setPrettyPrint(pretty);
            }

            @Override
            public Update update(String projectId, String datasetId, Dataset content) throws IOException {
                return super.update(projectId, datasetId, content).setPrettyPrint(pretty);
            }
        };
    }

    @Override
    public Jobs jobs() {
        return new Jobs() {
            @Override
            public Cancel cancel(String projectId, String jobId) throws IOException {
                return super.cancel(projectId, jobId).setPrettyPrint(pretty);
            }

            @Override
            public Get get(String projectId, String jobId) throws IOException {
                return super.get(projectId, jobId).setPrettyPrint(pretty);
            }

            @Override
            public GetQueryResults getQueryResults(String projectId, String jobId) throws IOException {
                return super.getQueryResults(projectId, jobId).setPrettyPrint(pretty);
            }

            @Override
            public Insert insert(String projectId, Job content) throws IOException {
                return super.insert(projectId, content).setPrettyPrint(pretty);
            }

            @Override
            public Insert insert(String projectId, Job content, AbstractInputStreamContent mediaContent) throws IOException {
                return super.insert(projectId, content, mediaContent).setPrettyPrint(pretty);
            }

            @Override
            public List list(String projectId) throws IOException {
                return super.list(projectId).setPrettyPrint(pretty);
            }

            @Override
            public Query query(String projectId, QueryRequest content) throws IOException {
                return super.query(projectId, content).setPrettyPrint(pretty);
            }
        };
    }

    @Override
    public Models models() {
        return new Models() {
            @Override
            public Delete delete(String projectId, String datasetId, String modelId) throws IOException {
                return super.delete(projectId, datasetId, modelId).setPrettyPrint(pretty);
            }

            @Override
            public Get get(String projectId, String datasetId, String modelId) throws IOException {
                return super.get(projectId, datasetId, modelId).setPrettyPrint(pretty);
            }

            @Override
            public List list(String projectId, String datasetId) throws IOException {
                return super.list(projectId, datasetId).setPrettyPrint(pretty);
            }

            @Override
            public Patch patch(String projectId, String datasetId, String modelId, Model content) throws IOException {
                return super.patch(projectId, datasetId, modelId, content).setPrettyPrint(pretty);
            }
        };
    }

    @Override
    public Projects projects() {
        return new Projects() {
            @Override
            public GetServiceAccount getServiceAccount(String projectId) throws IOException {
                return super.getServiceAccount(projectId).setPrettyPrint(pretty);
            }

            @Override
            public List list() throws IOException {
                return super.list().setPrettyPrint(pretty);
            }
        };
    }

    @Override
    public Routines routines() {
        return new Routines() {
            @Override
            public Delete delete(String projectId, String datasetId, String routineId) throws IOException {
                return super.delete(projectId, datasetId, routineId).setPrettyPrint(pretty);
            }

            @Override
            public Get get(String projectId, String datasetId, String routineId) throws IOException {
                return super.get(projectId, datasetId, routineId).setPrettyPrint(pretty);
            }

            @Override
            public Insert insert(String projectId, String datasetId, Routine content) throws IOException {
                return super.insert(projectId, datasetId, content).setPrettyPrint(pretty);
            }

            @Override
            public List list(String projectId, String datasetId) throws IOException {
                return super.list(projectId, datasetId).setPrettyPrint(pretty);
            }

            @Override
            public Update update(String projectId, String datasetId, String routineId, Routine content) throws IOException {
                return super.update(projectId, datasetId, routineId, content).setPrettyPrint(pretty);
            }
        };
    }

    @Override
    public Tabledata tabledata() {
        return new Tabledata() {
            @Override
            public InsertAll insertAll(String projectId, String datasetId, String tableId, TableDataInsertAllRequest content) throws IOException {
                return super.insertAll(projectId, datasetId, tableId, content).setPrettyPrint(pretty);
            }

            @Override
            public List list(String projectId, String datasetId, String tableId) throws IOException {
                return super.list(projectId, datasetId, tableId).setPrettyPrint(pretty);
            }
        };
    }

    @Override
    public Tables tables() {
        return new Tables() {
            @Override
            public Delete delete(String projectId, String datasetId, String tableId) throws IOException {
                return super.delete(projectId, datasetId, tableId).setPrettyPrint(pretty);
            }

            @Override
            public Get get(String projectId, String datasetId, String tableId) throws IOException {
                return super.get(projectId, datasetId, tableId).setPrettyPrint(pretty);
            }

            @Override
            public GetIamPolicy getIamPolicy(String resource, GetIamPolicyRequest content) throws IOException {
                return super.getIamPolicy(resource, content).setPrettyPrint(pretty);
            }

            @Override
            public Insert insert(String projectId, String datasetId, Table content) throws IOException {
                return super.insert(projectId, datasetId, content).setPrettyPrint(pretty);
            }

            @Override
            public List list(String projectId, String datasetId) throws IOException {
                return super.list(projectId, datasetId).setPrettyPrint(pretty);
            }

            @Override
            public Patch patch(String projectId, String datasetId, String tableId, Table content) throws IOException {
                return super.patch(projectId, datasetId, tableId, content).setPrettyPrint(pretty);
            }

            @Override
            public SetIamPolicy setIamPolicy(String resource, SetIamPolicyRequest content) throws IOException {
                return super.setIamPolicy(resource, content).setPrettyPrint(pretty);
            }

            @Override
            public TestIamPermissions testIamPermissions(String resource, TestIamPermissionsRequest content) throws IOException {
                return super.testIamPermissions(resource, content).setPrettyPrint(pretty);
            }

            @Override
            public Update update(String projectId, String datasetId, String tableId, Table content) throws IOException {
                return super.update(projectId, datasetId, tableId, content).setPrettyPrint(pretty);
            }
        };
    }
}
