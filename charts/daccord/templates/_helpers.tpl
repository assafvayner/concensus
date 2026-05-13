{{/*
Expand the name of the chart.
*/}}
{{- define "daccord.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this.
The StatefulSet pod names will be `<fullname>-0`, `<fullname>-1`, etc., so this
must leave room for the ordinal suffix and the headless DNS suffix.
*/}}
{{- define "daccord.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Headless service name (used as StatefulSet.spec.serviceName and for peer DNS).
*/}}
{{- define "daccord.headlessName" -}}
{{- printf "%s-headless" (include "daccord.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
ConfigMap name carrying the PEERS env var.
*/}}
{{- define "daccord.peersConfigMapName" -}}
{{- printf "%s-peers" (include "daccord.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "daccord.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "daccord.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "daccord.selectorLabels" -}}
app.kubernetes.io/name: {{ include "daccord.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Resolved image reference, falling back to .Chart.AppVersion when tag is empty.
*/}}
{{- define "daccord.image" -}}
{{- $repo := required "image.repository must be set in values.yaml" .Values.image.repository -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}

{{/*
Map values.yaml `algorithm` to the ALGORITHM env var the binary expects.
The binary only accepts "paxos" or "raft" — "multi-paxos" is a compile-time
feature that produces the optimized Paxos variant, so we map it to "paxos".
*/}}
{{- define "daccord.algorithmEnv" -}}
{{- $a := .Values.algorithm | default "raft" -}}
{{- if eq $a "raft" -}}raft{{- else -}}paxos{{- end -}}
{{- end -}}

{{/*
Comma-separated PEERS string consumed by the daccord-node binary.
Form: <fullname>-N=<fullname>-N.<headless>:<consensusPort>,...
The binary self-filters its own NodeId, so all pods can share this string.
*/}}
{{- define "daccord.peersString" -}}
{{- $full := include "daccord.fullname" . -}}
{{- $headless := include "daccord.headlessName" . -}}
{{- $port := .Values.ports.consensus | int -}}
{{- $entries := list -}}
{{- range $i, $_ := until (.Values.replicaCount | int) -}}
{{- $entry := printf "%s-%d=%s-%d.%s:%d" $full $i $full $i $headless $port -}}
{{- $entries = append $entries $entry -}}
{{- end -}}
{{- join "," $entries -}}
{{- end -}}
