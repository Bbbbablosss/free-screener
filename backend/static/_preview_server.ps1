$ErrorActionPreference='Stop'; $root=$PSScriptRoot; $port=8777
$l=New-Object System.Net.HttpListener; $l.Prefixes.Add("http://localhost:$port/"); $l.Start()
$mime=@{'.html'='text/html; charset=utf-8';'.js'='text/javascript';'.css'='text/css';'.json'='application/json';'.svg'='image/svg+xml';'.png'='image/png';'.ico'='image/x-icon'}
while($l.IsListening){ try{ $ctx=$l.GetContext(); $req=$ctx.Request; $res=$ctx.Response
  $rel=[System.Uri]::UnescapeDataString($req.Url.AbsolutePath.TrimStart('/')); if([string]::IsNullOrWhiteSpace($rel)){$rel='index.html'}
  $full=Join-Path $root $rel
  if($rel.StartsWith('api/') -or -not (Test-Path $full -PathType Leaf)){ $res.StatusCode=if($rel.StartsWith('api/')){200}else{404}; $res.ContentType='application/json'; $b=[Text.Encoding]::UTF8.GetBytes('{}'); $res.OutputStream.Write($b,0,$b.Length); $res.OutputStream.Close(); continue }
  $ext=[IO.Path]::GetExtension($full).ToLower(); if($mime.ContainsKey($ext)){$res.ContentType=$mime[$ext]}
  $by=[IO.File]::ReadAllBytes($full); $res.OutputStream.Write($by,0,$by.Length); $res.OutputStream.Close()
 }catch{ try{$ctx.Response.Close()}catch{} } }
