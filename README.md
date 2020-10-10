# starschema-bigquery-jdbc

![Maven Central](https://img.shields.io/maven-central/v/com.github.jonathanswenson/bqjdbc)

A fat (shaded) jar is provided at the following coordinates:
```xml
<dependency>
    <groupId>com.github.jonathanswenson</groupId>
    <artifactId>bqjdbc</artifactId>
    <version>...</version>
</dependency>
```

If you would like a thin jar you can use _thin_ classifier.

```xml
<dependency>
    <groupId>com.github.jonathanswenson</groupId>
    <artifactId>bqjdbc</artifactId>
    <version>...</version>
    <classifier>thin</classifier>
</dependency>
```

## Releases

Releases are handled through GitHub actions, and kicked off when a release is created.

> 💡 Make sure that  `-SNAPSHOT` is not part of the version when you create a release.

1. Prepare a release by removing `-SNAPSHOT` from the version in _pom.xml_

2. Initiate a release and be sure to write a meaningful description.

    > This will also create a tag with the specified name

    ![Creating a release](./create_release.png)

3. Check the GitHub action to see that it was a success

    ![Verify action is successful](./github_action_success.png)

4. Create a new commit by bumping the version and adding `-SNAPSHOT` to it