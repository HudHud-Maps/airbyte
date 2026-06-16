# syntax=docker/dockerfile:1
# check=skip=InvalidDefaultArgInFrom

# Self-contained multi-stage build for the destination-s3 connector image.
#
# Built by Kaniko through the HudHud cicd-reusable-workflows. Kaniko builds from
# a Dockerfile plus the git repository context, so the Gradle distTar build has
# to happen inside this Dockerfile.

# ---------------------------------------------------------------------------
# Builder stage: compile the connector distribution tar through Gradle.
# ---------------------------------------------------------------------------
FROM gradle:8.14-jdk21 AS builder

# Airbyte's root settings.gradle configures the full monorepo. Give Gradle
# enough heap for project configuration and disable the daemon for container CI.
ENV GRADLE_OPTS="-Dorg.gradle.jvmargs=-Xmx4g -Dorg.gradle.daemon=false -Dorg.gradle.welcome=never"

WORKDIR /build

# Copy the whole repo because destination-s3 depends on local Airbyte CDK modules.
COPY . .

RUN gradle :airbyte-integrations:connectors:destination-s3:distTar --no-daemon

# ---------------------------------------------------------------------------
# Runtime stage: package the dist on top of the Airbyte Java connector base.
# Mirrors docker-images/Dockerfile.java-connector while keeping this Kaniko build
# self-contained.
# ---------------------------------------------------------------------------
FROM docker.io/airbyte/java-connector-base:2.0.1@sha256:ec89bd1a89e825514dd2fc8730ba299a3ae1544580a078df0e35c5202c2085b3

USER root

# Set permissions for files provided by the base image.
RUN chmod +x /airbyte/base.sh /airbyte/javabase.sh && \
    chown airbyte:airbyte /airbyte/base.sh /airbyte/javabase.sh /airbyte/dd-java-agent.jar

ENV AIRBYTE_ENTRYPOINT="/airbyte/base.sh"
ENV APPLICATION="destination-s3"

# Preserve destination-s3's connector-specific final image setup.
COPY airbyte-integrations/connectors/destination-s3/finalize_build.sh /tmp/finalize_build.sh
RUN chmod +x /tmp/finalize_build.sh && \
    /tmp/finalize_build.sh && \
    rm -f /tmp/finalize_build.sh

# Add the connector dist tar and extract it. distTar produces one *.tar in the
# distributions directory, so match by glob instead of hardcoding the archive name.
COPY --from=builder /build/airbyte-integrations/connectors/destination-s3/build/distributions/ /tmp/distributions/
RUN set -e; \
    TAR="$(ls /tmp/distributions/*.tar | head -n1)" && \
    tar xf "$TAR" --strip-components=1 -C /airbyte && \
    rm -rf /tmp/distributions && \
    chown -R airbyte:airbyte /airbyte

USER airbyte

ENTRYPOINT ["/airbyte/base.sh"]
