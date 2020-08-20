---
title: "Quickstart"
date: 2020-04-21T20:46:17-04:00
weight: 3
---

## Introduction to guide

Includes what the guide covers, and a disclaimer that this is not suitable for production use, and an overview of warnings you might expect to see when running on Docker for Mac/Windows. Explains what the docker image consists of and how these are often separate instances.

Also covers the example use case for this (and other) example code, which is a microservices-based ecommerce example.

## Prerequisites

-   Docker
    -   system requirements
-   JQ (for formatting output, not essential)
-   cURL or wget

## Starting Docker container

Explains how to start the Docker container, and the various parameters set when starting it.

## Naming and placements

An overview of what namespacing and placements mean in the context of m3, where to find more details and how they relate to the configuration file this Docker image uses.

Covers some configuration options such as retention time that determines how long M3 keep metrics for, and naming strategies for namespaces.

## Configuration and sample config

Gives an overview of the configuration the Docker image uses, covering the most important concepts to understand at this point, and where to find more information for configuring each of the M3DB components.

<!-- TODO: more here? I feel like this config is glossed over -->

## Create namespace and placement

Starts with an explanation of how you could create a namespace and placement (plus links for more information), but that there is an endpoint you can use that does both for you in a single call.

<!-- NOTE: local refers to etcd cluster, as opposed to one elsewhere -->

### Placement initialization

What is placement initialization, what happens when you create one (sharding etc), and how to check status.

## Writing and querying metrics

Covers query engines M3 supports, and which this example uses (Prometheus).

### Writing metrics

Covers a quick overview of the difference between untagged metrics and tagged metrics, and using Prometheus reserved tags.

The example will write more, and more complex metrics than the current example

### Querying metrics

Covers querying metrics using PromQL, showing a handful of different queries that make sense related to the metrics example.
